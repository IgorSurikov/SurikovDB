from jsonschema import ValidationError


class JSONQLException(Exception):
    def __init__(self, error_list: list[ValidationError]):
        self.error_list = error_list
        self.message = 'JSONQL parse exception'
        super().__init__(self.message)

    def __str__(self):
        error_list_str = '\n'.join([error.message for error in self.error_list])
        return f"{self.message}:\n {error_list_str}"
