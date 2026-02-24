from typing import List, Dict, Optional, Set
from ..adapters.base import TableInfo, RelationshipInfo

class SchemaEnhancer:
    def __init__(self, enable_inference: bool = True, min_confidence: float = 0.7):
        self.enable_inference = enable_inference
        self.min_confidence = min_confidence

    def enhance_relationships(self, tables: List[TableInfo], existing_relationships: List[RelationshipInfo]) -> List[RelationshipInfo]:
        if not self.enable_inference:
            return existing_relationships

        enhanced = list(existing_relationships)
        existing_pairs = set(
            f"{r.from_table.lower()}.{r.from_columns[0].lower()}" 
            for r in existing_relationships
        )
        
        table_names = {t.name.lower() for t in tables}
        table_map = {t.name.lower(): t for t in tables}

        for table in tables:
            table_name_lower = table.name.lower()
            
            for col in table.columns:
                col_name_lower = col.name.lower()
                
                # Skip if already exists or is primary key
                if f"{table_name_lower}.{col_name_lower}" in existing_pairs:
                    continue
                if col.name in table.primary_keys:
                    continue

                inferred = self._try_infer_relation(
                    table.name, col.name, table_names, table_map
                )
                
                if inferred and inferred.confidence >= self.min_confidence:
                    enhanced.append(inferred)

        return enhanced

    def _try_infer_relation(
        self, from_table: str, from_col: str, table_names: Set[str], table_map: Dict[str, TableInfo]
    ) -> Optional[RelationshipInfo]:
        from_col_lower = from_col.lower()
        
        # Rule 1: xxx_id -> xxxs.id or xxx.id
        if from_col_lower.endswith('_id') and from_col_lower != 'id':
            base_name = from_col_lower[:-3]
            return self._find_target_table(from_table, from_col, base_name, 'id', table_names, table_map)
            
        # Rule 2: xxxId -> xxxs.id
        # Simple camelCase check could be added here if needed
        
        return None

    def _find_target_table(
        self, from_table: str, from_col: str, base_name: str, target_col: str, 
        table_names: Set[str], table_map: Dict[str, TableInfo], base_confidence: float = 0.95
    ) -> Optional[RelationshipInfo]:
        
        candidates = [
            (base_name + 's', base_confidence),
            (base_name + 'es', base_confidence),
            (base_name, base_confidence - 0.05)
        ]

        for target_table_name, confidence in candidates:
            if target_table_name in table_names and target_table_name != from_table.lower():
                target_table = table_map[target_table_name]
                
                # Check if target column exists (usually 'id')
                has_col = any(c.name.lower() == target_col for c in target_table.columns)
                
                if has_col:
                    return RelationshipInfo(
                        fromTable=from_table,
                        fromColumns=[from_col],
                        toTable=target_table.name,
                        toColumns=[target_col], # Assuming 'id' matches logic above
                        type='many-to-one',
                        source='inferred',
                        confidence=confidence
                    )
        return None
