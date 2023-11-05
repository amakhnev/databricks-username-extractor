import unittest
from libraries.username_generator import generate_usernames

class TestUsernameGeneration(unittest.TestCase):

    def test_incorporation_removal(self):
        self.assertEqual(generate_usernames("Example Limited")[0][0], 'example')

    def test_special_characters_handling(self):
        self.assertIn('abc', [username for username, score in generate_usernames("A&B&C LLP")])

    def test_length_constraints(self):
        usernames = generate_usernames("A Very Long Company Name That Exceeds The Usual Length Ltd")
        for username, score in usernames:
            self.assertTrue(3 <= len(username) <= 20)

    def test_relevance_score_exact_match(self):
        usernames = generate_usernames("ExactMatch Ltd")
        self.assertEqual(usernames[0], ('exactmatch', 100))

    def test_relevance_score_partial_match(self):
        usernames = generate_usernames("Partial Match Company Ltd")
        self.assertTrue(all(score < 100 for username, score in usernames if username != 'pmc'))

    def test_maximum_usernames(self):
        usernames = generate_usernames("A Company With A Fairly Long Name Ltd")
        self.assertTrue(len(usernames) <= 5)

    def test_acronym_generation(self):
        usernames = generate_usernames("Acronym Generating Company Ltd")
        self.assertIn(('agc', 100), usernames)

if __name__ == '__main__':
    unittest.main()