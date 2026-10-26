"""Independent allocation inventory; metadata is evidence, SQL is deletion authority."""

import asyncio


class AllocationInventory:
    def __init__(self, ecosystem):
        self.ecosystem = ecosystem
        self.manager = None

    async def scan(self):
        from opensandbox.exceptions import SandboxApiException
        from opensandbox.manager import SandboxManager
        from opensandbox.models.sandboxes import SandboxFilter

        if self.manager is None:
            self.manager = await SandboxManager.create(self.ecosystem.connection)
        candidates = await self.ecosystem.allocations.cleanup_candidates()
        if not candidates:
            return
        remote = []
        for page in range(1, 101):
            response = await self.manager.list_sandbox_infos(
                SandboxFilter(
                    metadata={"centn.ecosystem": self.ecosystem.config.deployment_id},
                    page=page,
                    page_size=100,
                )
            )
            remote.extend(response.sandbox_infos)
            if not response.pagination.has_next_page:
                break
        else:
            raise ValueError("Allocation inventory exceeded its bounded page count")

        async def absent(sandbox_id):
            try:
                await self.manager.get_sandbox_info(sandbox_id)
            except SandboxApiException as exc:
                if exc.status_code == 404:
                    return True
                raise
            return False

        for candidate in candidates:
            allocation = await self.ecosystem.allocations.claim_cleanup(
                candidate.id, candidate.scope_id
            )
            if allocation is None:
                continue
            matches = [
                info
                for info in remote
                if all(
                    (info.metadata or {}).get(key) == value
                    for key, value in allocation.metadata.items()
                )
            ]
            if allocation.sandbox_id is not None:
                if any(info.id != allocation.sandbox_id for info in matches):
                    raise ValueError("Allocation inventory contains conflicting sandbox identities")
                if not matches:
                    if await absent(allocation.sandbox_id):
                        await self.ecosystem.allocations.complete_cleanup(
                            allocation.id, allocation.scope_id
                        )
                    # A known ID with missing/mutated metadata is not deletion authority.
                    continue
            if not matches:
                # Create may still be in flight. No ID + an empty list is not proof
                # that no owned remote will appear; retain durable reconciliation.
                continue
            if len(matches) != 1:
                raise ValueError("One allocation has multiple remote sandbox identities")
            info = matches[0]
            if allocation.sandbox_id is None:
                await self.ecosystem.allocations.created(allocation, info.id)
            try:
                await self.manager.kill_sandbox(info.id)
            except SandboxApiException as exc:
                if exc.status_code != 404:
                    raise
            if await absent(info.id):
                await self.ecosystem.allocations.complete_cleanup(
                    allocation.id, allocation.scope_id
                )

    async def aclose(self):
        if self.manager is not None:
            await self.manager.close()


class InventoryMaintenanceService:
    """Run SQL execution supervision and inventory in the same independent process."""

    def __init__(self, service, ecosystem):
        self.service = service
        self.inventory = AllocationInventory(ecosystem)
        self.stop = asyncio.Event()

    def request_drain(self):
        self.stop.set()
        self.service.request_drain()

    async def scan_loop(self):
        while not self.stop.is_set():
            try:
                async with asyncio.timeout(25):
                    await self.inventory.scan()
            except Exception as exc:
                # Retain SQL cleanup work. Never convert an API failure to absence.
                print({"allocationInventoryErrorType": type(exc).__name__}, flush=True)
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=1)
            except TimeoutError:
                pass

    async def serve(self):
        service = asyncio.create_task(self.service.serve())
        inventory = asyncio.create_task(self.scan_loop())
        try:
            done, _ = await asyncio.wait({service, inventory}, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            self.request_drain()
            await asyncio.gather(service, inventory, return_exceptions=True)
            await self.inventory.aclose()
