from typing import Union, Tuple

BLOCK_SIZE = 4 * 1024

POINTER_F = 'L'
POINTER_COUNT_F = 'H'
ROW_COUNT_F = 'H'

TABLE_NAME_F = '50s'
COLUMN_COUNT_F = 'H'
COLUMN_NAME_F = '50s'
COLUMN_CODE_F = 'h'
TABLE_DDL_SIZE_F = 'H'
ROW_IS_DELETED_F = '?'


DB_TYPE = Union[bool, int, str]
ROW_TYPE = Tuple[DB_TYPE, ...]