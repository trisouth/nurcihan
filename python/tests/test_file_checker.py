import unittest
from src.file_checker import is_file_present

class TestFileChecker(unittest.TestCase):
    def test_files(self):
        test_cases = [
            ("path/to/existing/file1.csv", True),
            ("path/to/existing/file2.csv", True),
            ("path/to/nonexistent/file1.csv", False),
            ("path/to/nonexistent/file2.csv", False),
        ]

        for file_path, expected_result in test_cases:
            with self.subTest(file=file_path):
                self.assertEqual(is_file_present(file_path), expected_result)

if __name__ == "__main__":
    unittest.main()
