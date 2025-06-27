import re
import string
import nltk
from nltk.corpus import stopwords

from algorithms.SequentialSearch import SequentialSearch

nltk.download('punkt')
nltk.download('stopwords')

class Preprocessor:
    def __init__(self):
        self.s = SequentialSearch()
    # Tokenizer using whitespace
    def tokenize(self, text):
        return self.s.split(text)

    def preprocess_text(self, text):
        if not text:
            return []

        # Lowercase
        text = text.lower()

        # Split CamelCase words (e.g., 'QualificationsExperience' → 'Qualifications Experience')
        text = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', text)

        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)

        # Replace certain punctuations with space
        text = re.sub(r'[:/\-\\]', ' ', text)

        # Remove non-alphanumeric characters except space
        text = re.sub(r'[^a-z0-9\s]', '', text)

        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()

        # Tokenize
        tokens = self.tokenize(text)

        # Remove punctuation and stopwords
        tokens = [t for t in tokens if t not in string.punctuation]
        tokens = [t for t in tokens if t not in stopwords.words('english')]

        # Optional stemming
        # stemmer = PorterStemmer()
        # tokens = [stemmer.stem(t) for t in tokens]

        return tokens
