import random
import string


def generate_large_text_file(filename, size_mb):
    """
    Generate a large text file with random content.

    Args:
        filename (str): Output file name
        size_mb (float): Size in megabytes
    """
    # Target size in bytes
    target_size = int(size_mb * 1024 * 1024)

    # Common words to make text somewhat readable
    common_words = [
        "the",
        "be",
        "to",
        "of",
        "and",
        "a",
        "in",
        "that",
        "have",
        "I",
        "it",
        "for",
        "not",
        "on",
        "with",
        "he",
        "as",
        "you",
        "do",
        "at",
        "this",
        "but",
        "his",
        "by",
        "from",
        "they",
        "we",
        "say",
        "her",
        "she",
        "or",
        "an",
        "will",
        "my",
        "one",
        "all",
        "would",
        "there",
        "their",
        "what",
        "so",
        "up",
        "out",
        "if",
        "about",
        "who",
        "get",
        "which",
        "go",
        "me",
        "when",
        "make",
        "can",
        "like",
        "time",
        "no",
        "just",
        "him",
        "know",
        "take",
        "people",
        "into",
        "year",
        "your",
        "good",
        "some",
        "could",
        "them",
        "see",
        "other",
        "than",
        "then",
        "now",
        "look",
        "only",
        "come",
        "its",
        "over",
        "think",
        "also",
        "back",
        "after",
        "use",
        "two",
        "how",
        "our",
        "work",
        "first",
        "well",
        "way",
        "even",
        "new",
        "want",
        "because",
        "any",
        "these",
        "give",
        "day",
        "most",
        "us",
    ]

    print(f"Generating {filename} with target size: {size_mb} MB")

    with open(filename, "w", encoding="utf-8") as file:
        current_size = 0
        line_count = 0

        while current_size < target_size:
            # Create a line with random words and punctuation
            line_length = random.randint(50, 120)
            line = []

            while len(" ".join(line)) < line_length:
                # Sometimes use common words, sometimes random strings
                if random.random() < 0.7:  # 70% chance for common words
                    word = random.choice(common_words)
                else:
                    word_length = random.randint(3, 10)
                    word = "".join(
                        random.choices(string.ascii_lowercase, k=word_length)
                    )

                line.append(word)

            # Add some punctuation
            if random.random() < 0.3:
                line[-1] += random.choice([".", "!", "?"])
            elif random.random() < 0.2:
                line[-1] += ","

            final_line = " ".join(line) + "\n"
            file.write(final_line)

            current_size += len(final_line.encode("utf-8"))
            line_count += 1

            # Progress indicator
            if line_count % 1000 == 0:
                progress = (current_size / target_size) * 100
                print(
                    f"Progress: {progress:.1f}% ({current_size / (1024 * 1024):.2f} MB)"
                )

    print(f"File generated: {filename}")
    print(f"Final size: {current_size / (1024 * 1024):.2f} MB")
    print(f"Total lines: {line_count}")


if __name__ == "__main__":
    # Generate text file for word count map reduce jobs
    # file dir
    file_dir = "./test_file"
    generate_large_text_file(file_dir + "/small.txt", 4.0)
    generate_large_text_file(file_dir + "/med.txt", 16.0)
    generate_large_text_file(file_dir + "/large.txt", 256.0)

