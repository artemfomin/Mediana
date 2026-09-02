"""Generate the Mediana vector and NuGet raster icons without dependencies."""

from __future__ import annotations

import math
import struct
import zlib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SVG_PATH = ROOT / "icon.svg"
PNG_PATH = ROOT / "icon.png"

SIZE = 512
PNG_SIZE = 128
SUPERSAMPLE = 4

BACKGROUND = "#18191B"
LIME = "#AEF500"
GRAY = "#A6A8AB"

BACKGROUND_LIGHT_RGBA = (42, 44, 48, 255)
BACKGROUND_DARK_RGBA = (20, 21, 23, 255)
LIME_LIGHT_RGBA = (196, 255, 18, 255)
LIME_DARK_RGBA = (140, 203, 0, 255)
GRAY_LIGHT_RGBA = (198, 200, 204, 255)
GRAY_DARK_RGBA = (119, 122, 128, 255)

STROKE_WIDTH = 58.0
BACKGROUND_BOX = (16.0, 16.0, 496.0, 496.0)
BACKGROUND_RADIUS = 96.0

# Four continuous angular ribbons reproduce concept 12 from the palette board.
# Their inner ends overlap in the middle instead of terminating as visible dots.
LIME_TOP = ((88.0, 236.0), (176.0, 160.0), (192.0, 152.0), (208.0, 160.0), (296.0, 248.0))
GRAY_TOP = ((216.0, 248.0), (304.0, 160.0), (320.0, 152.0), (336.0, 160.0), (424.0, 236.0))
GRAY_BOTTOM = ((88.0, 276.0), (176.0, 352.0), (192.0, 360.0), (208.0, 352.0), (296.0, 264.0))
LIME_BOTTOM = ((216.0, 264.0), (304.0, 352.0), (320.0, 360.0), (336.0, 352.0), (424.0, 276.0))

PAINT_ORDER = (
    ("lime", LIME_TOP),
    ("gray", GRAY_TOP),
    ("gray", GRAY_BOTTOM),
    ("lime", LIME_BOTTOM),
)


def svg_path(points: tuple[tuple[float, float], ...]) -> str:
    head, *tail = points
    return "M " + " L ".join(
        [f"{head[0]:g} {head[1]:g}", *(f"{x:g} {y:g}" for x, y in tail)]
    )


def make_svg() -> str:
    return f'''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {SIZE} {SIZE}" role="img" aria-labelledby="title desc">
  <title id="title">Mediana</title>
  <desc id="desc">Two interlocking angular links representing mediation between components.</desc>
  <defs>
    <radialGradient id="background" cx="36%" cy="30%" r="88%">
      <stop offset="0" stop-color="#2A2C30"/>
      <stop offset="1" stop-color="#141517"/>
    </radialGradient>
    <linearGradient id="lime" x1="88" y1="152" x2="424" y2="360" gradientUnits="userSpaceOnUse">
      <stop offset="0" stop-color="#C4FF12"/>
      <stop offset="0.5" stop-color="{LIME}"/>
      <stop offset="1" stop-color="#8CCB00"/>
    </linearGradient>
    <linearGradient id="gray" x1="88" y1="152" x2="424" y2="360" gradientUnits="userSpaceOnUse">
      <stop offset="0" stop-color="#C6C8CC"/>
      <stop offset="0.5" stop-color="{GRAY}"/>
      <stop offset="1" stop-color="#777A80"/>
    </linearGradient>
  </defs>
  <rect x="16" y="16" width="480" height="480" rx="96" fill="url(#background)"/>
  <g fill="none" stroke-width="{STROKE_WIDTH:g}" stroke-linecap="round" stroke-linejoin="round">
    <path d="{svg_path(LIME_TOP)}" stroke="url(#lime)"/>
    <path d="{svg_path(GRAY_TOP)}" stroke="url(#gray)"/>
    <path d="{svg_path(GRAY_BOTTOM)}" stroke="url(#gray)"/>
    <path d="{svg_path(LIME_BOTTOM)}" stroke="url(#lime)"/>
  </g>
</svg>
'''


def distance_to_segment(px: float, py: float, start: tuple[float, float], end: tuple[float, float]) -> float:
    ax, ay = start
    bx, by = end
    vx = bx - ax
    vy = by - ay
    length_squared = vx * vx + vy * vy
    if length_squared == 0:
        return math.hypot(px - ax, py - ay)
    projection = ((px - ax) * vx + (py - ay) * vy) / length_squared
    projection = min(1.0, max(0.0, projection))
    return math.hypot(px - (ax + projection * vx), py - (ay + projection * vy))


def hits_stroke(px: float, py: float, points: tuple[tuple[float, float], ...]) -> bool:
    radius = STROKE_WIDTH / 2.0
    return any(
        distance_to_segment(px, py, points[index], points[index + 1]) <= radius
        for index in range(len(points) - 1)
    )


def inside_rounded_rect(px: float, py: float) -> bool:
    left, top, right, bottom = BACKGROUND_BOX
    if not (left <= px <= right and top <= py <= bottom):
        return False
    closest_x = min(max(px, left + BACKGROUND_RADIUS), right - BACKGROUND_RADIUS)
    closest_y = min(max(py, top + BACKGROUND_RADIUS), bottom - BACKGROUND_RADIUS)
    dx = px - closest_x
    dy = py - closest_y
    return dx * dx + dy * dy <= BACKGROUND_RADIUS * BACKGROUND_RADIUS


def interpolate_color(
    start: tuple[int, int, int, int], end: tuple[int, int, int, int], amount: float
) -> tuple[int, int, int, int]:
    amount = min(1.0, max(0.0, amount))
    return tuple(round(left + (right - left) * amount) for left, right in zip(start, end))  # type: ignore[return-value]


def background_color(px: float, py: float) -> tuple[int, int, int, int]:
    distance = math.hypot(px - 184.0, py - 154.0) / 450.0
    return interpolate_color(BACKGROUND_LIGHT_RGBA, BACKGROUND_DARK_RGBA, distance)


def stroke_color(kind: str, px: float, py: float) -> tuple[int, int, int, int]:
    amount = ((px - 88.0) + (py - 152.0)) / 544.0
    if kind == "lime":
        return interpolate_color(LIME_LIGHT_RGBA, LIME_DARK_RGBA, amount)
    return interpolate_color(GRAY_LIGHT_RGBA, GRAY_DARK_RGBA, amount)


def sample(px: float, py: float) -> tuple[int, int, int, int]:
    if not inside_rounded_rect(px, py):
        return (0, 0, 0, 0)
    color = background_color(px, py)
    for kind, points in PAINT_ORDER:
        if hits_stroke(px, py, points):
            color = stroke_color(kind, px, py)
    return color


def png_chunk(tag: bytes, data: bytes) -> bytes:
    payload = struct.pack(">I", len(data)) + tag + data
    return payload + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)


def make_png() -> bytes:
    rows: list[bytes] = []
    scale = SIZE / PNG_SIZE
    samples_per_pixel = SUPERSAMPLE * SUPERSAMPLE

    for y in range(PNG_SIZE):
        row = bytearray((0,))
        for x in range(PNG_SIZE):
            alpha_sum = 0
            red_sum = green_sum = blue_sum = 0
            for sy in range(SUPERSAMPLE):
                py = (y + (sy + 0.5) / SUPERSAMPLE) * scale
                for sx in range(SUPERSAMPLE):
                    px = (x + (sx + 0.5) / SUPERSAMPLE) * scale
                    red, green, blue, alpha = sample(px, py)
                    alpha_sum += alpha
                    red_sum += red * alpha
                    green_sum += green * alpha
                    blue_sum += blue * alpha

            alpha = round(alpha_sum / samples_per_pixel)
            if alpha_sum:
                red = round(red_sum / alpha_sum)
                green = round(green_sum / alpha_sum)
                blue = round(blue_sum / alpha_sum)
            else:
                red = green = blue = 0
            row.extend((red, green, blue, alpha))
        rows.append(bytes(row))

    raw = b"".join(rows)
    png = b"\x89PNG\r\n\x1a\n"
    png += png_chunk(b"IHDR", struct.pack(">IIBBBBB", PNG_SIZE, PNG_SIZE, 8, 6, 0, 0, 0))
    png += png_chunk(b"IDAT", zlib.compress(raw, 9))
    png += png_chunk(b"IEND", b"")
    return png


def main() -> None:
    SVG_PATH.write_text(make_svg(), encoding="utf-8")
    PNG_PATH.write_bytes(make_png())
    print(f"{SVG_PATH.name}: {SVG_PATH.stat().st_size} bytes")
    print(f"{PNG_PATH.name}: {PNG_PATH.stat().st_size} bytes")


if __name__ == "__main__":
    main()
