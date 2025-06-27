class SequentialSearch:
    def __init__(self):
        pass

    def split(self, text, delimiter=None):
        if not isinstance(text, str):
            raise TypeError("Input 'text' must be a string.")

        if not text:
            return []

        results = []
        current_word_start = 0

        if delimiter == None or delimiter == '':
            is_in_whitespace_block = True
            for i in range(len(text)):
                if text[i].isspace():
                    if not is_in_whitespace_block:
                        results.append(text[current_word_start:i])
                        is_in_whitespace_block = True
                    current_word_start = i + 1
                else:
                    if is_in_whitespace_block:
                        current_word_start = i
                        is_in_whitespace_block = False
            if not is_in_whitespace_block:
                results.append(text[current_word_start:])
        else:
            delimiter_length = len(delimiter)
            i = 0
            while i <= len(text) - delimiter_length:
                if text[i:i + delimiter_length] == delimiter:
                    results.append(text[current_word_start:i])
                    current_word_start = i + delimiter_length
                    i += delimiter_length
                else:
                    i += 1
            results.append(text[current_word_start:])

        return results