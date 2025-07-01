def word_frequencies(paragraph):
    freq = {}
    for row in paragraph:
        for word in row:
            if len(word) < 3:
                continue
            if word.lower() == "stop":
                break
            word_lower = word.lower()
            freq[word_lower] = freq.get(word_lower, 0) + 1
        else:
            continue
        break
    return freq

paragraph = [
    ["Hello", "world", "hello"],
    ["this", "is", "a", "test"],
    ["STOP", "ignore", "this", "line"],
    ["should", "not", "be", "processed"]
]

print(word_frequencies(paragraph))
