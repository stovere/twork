def convert_duration_to_seconds(duration: str) -> int:
    parts = list(map(int, duration.split(":")))
    return sum(x * 60 ** i for i, x in enumerate(reversed(parts)))

def convert_to_bytes(size_str: str) -> int:
    unit_to_bytes = {
        'B': 1,
        'KB': 1024,
        'MB': 1024 ** 2,
        'GB': 1024 ** 3,
        'TB': 1024 ** 4
    }
    try:
        size, unit = size_str.split()
        return int(float(size) * unit_to_bytes[unit.upper()])
    except Exception as e:
        print(f"Error: {e}")
        return 0
