import re
from typing import List, Optional
from ..adapters.base import DbConfig

# Operation keywords mapping
OPERATION_KEYWORDS = {
    'insert': ['INSERT', 'REPLACE'],
    'update': ['UPDATE'],
    'delete': ['DELETE', 'TRUNCATE'],
    'ddl': ['CREATE', 'ALTER', 'DROP', 'RENAME', 'GRANT', 'REVOKE'],
}

PERMISSION_PRESETS = {
    'safe': ['read'],
    'readwrite': ['read', 'insert', 'update'],
    'full': ['read', 'insert', 'update', 'delete', 'ddl'],
}

def resolve_permissions(config: DbConfig) -> List[str]:
    # Custom permissions priority
    if config.permissions:
        perms = set(['read'] + config.permissions)
        return list(perms)
    
    # Use preset mode
    if config.permission_mode and config.permission_mode != 'custom':
        return PERMISSION_PRESETS.get(config.permission_mode, PERMISSION_PRESETS['safe'])
    
    return PERMISSION_PRESETS['safe']

def starts_with_keyword(query: str, keyword: str) -> bool:
    # Remove comments and leading whitespace
    # Simple regex to remove -- comments and /* */ comments
    # Note: A full SQL parser would be safer but regex covers most accidental cases
    clean_query = re.sub(r'--.*', '', query)
    clean_query = re.sub(r'/\*.*?\*/', '', clean_query, flags=re.DOTALL)
    clean_query = clean_query.strip()
    
    pattern = re.compile(rf'^{keyword}\b', re.IGNORECASE)
    return bool(pattern.match(clean_query))

def is_write_operation(query: str) -> bool:
    upper_query = query.strip().upper()
    for keywords in OPERATION_KEYWORDS.values():
        for keyword in keywords:
            if starts_with_keyword(upper_query, keyword):
                return True
    return False

def detect_operation_type(query: str) -> Optional[dict]:
    upper_query = query.strip().upper()
    for op_type, keywords in OPERATION_KEYWORDS.items():
        for keyword in keywords:
            if starts_with_keyword(upper_query, keyword):
                return {'type': op_type, 'keyword': keyword}
    return None

def validate_query(query: str, config: DbConfig) -> None:
    permissions = resolve_permissions(config)
    
    detected = detect_operation_type(query)
    if detected and detected['type'] not in permissions:
        labels = {
            'insert': 'INSERT(insert)',
            'update': 'UPDATE(update)',
            'delete': 'DELETE(delete)',
            'ddl': 'DDL(ddl)',
        }
        raise ValueError(
            f"❌ Operation refused: Current permissions do not allow {detected['keyword']} operations."
            f"Required permission: {labels.get(detected['type'], detected['type'])}"
            f"Current permissions: {', '.join(permissions)}"
            "To enable, check your --permission-mode or --permissions configuration."
        )
