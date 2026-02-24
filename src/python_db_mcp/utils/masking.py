import re
from typing import Any, List, Dict, Union

class DataMasker:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.rules = [
            (re.compile(r'password|passwd|secret|token', re.I), '******'),
            (re.compile(r'email', re.I), lambda x: self._mask_email(str(x))),
            (re.compile(r'phone|mobile', re.I), lambda x: self._mask_phone(str(x))),
        ]

    def _mask_email(self, email: str) -> str:
        if '@' not in email: return email
        user, domain = email.split('@')
        return f"{user[:2]}***@{domain}"

    def _mask_phone(self, phone: str) -> str:
        if len(phone) < 7: return phone
        return f"{phone[:3]}****{phone[-4:]}"

    def mask_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        if not self.enabled: return row
        
        masked = row.copy()
        for key, value in row.items():
            for pattern, mask_fn in self.rules:
                if pattern.search(key):
                    if callable(mask_fn):
                        masked[key] = mask_fn(value)
                    else:
                        masked[key] = mask_fn
                    break
        return masked

    def mask_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self.mask_row(row) for row in rows]
