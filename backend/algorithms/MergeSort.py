# Text preprocessing utils
class MergeSort():
    def __init__(self):
        pass

    def merge_sort(self, arr):
        if len(arr) <= 1:
            return arr  # base case: already sorted

        # Split the array into two halves
        mid = len(arr) // 2
        left_half = self.merge_sort(arr[:mid])
        right_half = self.merge_sort(arr[mid:])

        # Merge the sorted halves
        return self.merge(left_half, right_half)

    def merge(self, left, right):
        result = []
        i = j = 0

        while i < len(left) and j < len(right):
            if left[i][1] >= right[j][1]:  # sort by score descending
                result.append(left[i])
                i += 1
            else:
                result.append(right[j])
                j += 1

        result.extend(left[i:])
        result.extend(right[j:])
        return result