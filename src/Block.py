from constants import BLOCK_SIZE, BYTEORDER, POINTER_SIZE, POINTERS_PER_BLOCK

class Block(bytearray):

    def __init__(self, data: bytes, index: int = 0):
        assert len(data) == BLOCK_SIZE, 'Incorrect block size: {}'.format(len(data))
        super().__init__(data)

        self.idx = index

    @classmethod
    def empty(cls, index: int = 0) -> 'Block':
        return cls(b'\0' * BLOCK_SIZE, index)

    def override(self, offset: int, data: bytes) -> None:
        assert offset + len(data) <= BLOCK_SIZE

        self[offset:offset + len(data)] = data

    def get_pointer(self, index: int) -> int:
        assert 0 <= index < POINTERS_PER_BLOCK

        binary = self[index * POINTER_SIZE:(index + 1) * POINTER_SIZE]
        pointer = int.from_bytes(binary, byteorder=BYTEORDER)

        return pointer

    def set_pointer(self, index: int, pointer: int) -> None:
        assert 0 <= index < POINTERS_PER_BLOCK

        binary = pointer.to_bytes(POINTER_SIZE, byteorder=BYTEORDER)
        self.override(index * POINTER_SIZE, binary)



