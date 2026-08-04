def format_file_size(size_in_bytes: int | None) -> str:
    """Converts bytes to a human-readable format."""
    if size_in_bytes is None:
        return "unknown size"
        
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_in_bytes < 1024.0:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024.0
        
    return f"{size_in_bytes:.2f} PB"