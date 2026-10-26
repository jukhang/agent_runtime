"""Dependency-free raster bar chart from the supplied workshop dataset."""

import json
import os
import struct
import sys
import zlib
from pathlib import Path

assert os.getuid() == 1000 and os.getgroups() == []
assert not Path("/var/run/docker.sock").exists()
assert "OPENSANDBOX_SERVER_API_KEY" not in os.environ
data = json.loads(sys.argv[1])
assert set(data) == {"datasetId", "rows"}
assert isinstance(data["datasetId"], str) and len(data["datasetId"]) <= 80
rows = data["rows"]
assert 1 <= len(rows) <= 12
for row in rows:
    assert set(row) == {"label", "planned", "actual"}
    assert isinstance(row["label"], str) and 1 <= len(row["label"]) <= 80
    assert all(type(row[k]) is int and 0 <= row[k] <= 1000000 for k in ("planned", "actual"))
width, height = 800, 480
pixels = bytearray([255] * width * height * 3)


def rectangle(x0, y0, x1, y1, color):
    for y in range(y0, y1):
        start = (y * width + x0) * 3
        pixels[start : start + (x1 - x0) * 3] = bytes(color) * (x1 - x0)


FONT = dict(
    zip(
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ .:%-",
        (
            "111101101101111",
            "010110010010111",
            "111001111100111",
            "111001111001111",
            "101101111001001",
            "111100111001111",
            "111100111101111",
            "111001010010010",
            "111101111101111",
            "111101111001111",
            "010101111101101",
            "110101110101110",
            "111100100100111",
            "110101101101110",
            "111100110100111",
            "111100110100100",
            "111100101101111",
            "101101111101101",
            "111010010010111",
            "001001001101111",
            "101101110101101",
            "100100100100111",
            "101111111101101",
            "101111111111101",
            "111101101101111",
            "111101111100100",
            "111101101111001",
            "110101110101101",
            "111100111001111",
            "111010010010010",
            "101101101101111",
            "101101101101010",
            "101101111111101",
            "101101010101101",
            "101101010010010",
            "111001010100111",
            "000000000000000",
            "000000000000010",
            "000010000010000",
            "101001010100101",
            "000000111000000",
        ),
        strict=True,
    )
)


def text(x, y, value, color=(40, 40, 40), scale=2):
    for index, char in enumerate(str(value).upper()):
        for cell, bit in enumerate(FONT.get(char, FONT[" "])):
            if bit == "1":
                px, py = x + index * 4 * scale + cell % 3 * scale, y + cell // 3 * scale
                rectangle(px, py, px + scale, py + scale, color)


text(60, 10, "WORKSHOP METRICS")
text(450, 10, "PLANNED", (30, 145, 160))
text(570, 10, "ACTUAL", (220, 75, 100))


rectangle(59, 35, 61, 421, (40, 40, 40))
rectangle(59, 420, 761, 422, (40, 40, 40))
ceiling = max(1, max(max(r["planned"], r["actual"]) for r in rows))
stride = 680 // len(rows)
bars = []
for index, row in enumerate(rows):
    x = 75 + stride * index
    bw = min(38, (stride - 12) // 2)
    text(x, 440, row["label"][:20], scale=1)
    for offset, key, color in ((0, "planned", (30, 145, 160)), (bw + 3, "actual", (220, 75, 100))):
        top = 420 - round(row[key] / ceiling * 360)
        rectangle(x + offset, top, x + offset + bw, 420, color)
        text(x + offset, max(35, top - 13), row[key], color, scale=1)
        bars.append(
            {
                "label": row["label"],
                "series": key,
                "value": row[key],
                "rect": [x + offset, top, x + offset + bw, 420],
            }
        )


def chunk(kind, body):
    return struct.pack(">I", len(body)) + kind + body + struct.pack(">I", zlib.crc32(kind + body))


scanlines = b"".join(b"\x00" + pixels[y * width * 3 : (y + 1) * width * 3] for y in range(height))
png = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
png += chunk(b"tEXt", b"Dataset\x00" + json.dumps(data, ensure_ascii=True).encode())
png += chunk(b"IDAT", zlib.compress(scanlines)) + chunk(b"IEND", b"")
Path("/artifacts/chart.png").write_bytes(png)
planned, actual = sum(r["planned"] for r in rows), sum(r["actual"] for r in rows)
report = {
    "data": data,
    "uid": os.getuid(),
    "planned": planned,
    "actual": actual,
    "ratio": actual / planned if planned else None,
    "bars": bars,
    "legend": {"planned": "teal", "actual": "red"},
    "size": [width, height],
}
Path("/artifacts/report.json").write_text(json.dumps(report, ensure_ascii=True))
print(
    json.dumps({"datasetId": data["datasetId"], "planned": planned, "actual": actual}), flush=True
)
