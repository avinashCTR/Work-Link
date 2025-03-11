import itertools
import time
from collections import defaultdict


class PhoneticsGenerator:
    arr_rules = {
        "1": ["l", "I"], "l": ["1", "ll"], "I": ["1"], "6": ["b"], "b": ["6"],
        "0": ["O", "o"], "O": ["0"], "o": ["0", "ou"], "y": ["i", "j"],
        "i": ["y"], "c": ["ck"], "k": ["ck"], "f": ["ph"], "ph": ["f"],
        "g": ["dzh"], "dzh": ["g", "j"], "j": ["dzh"], "h": ["kh"], "kh": ["h"],
        "d": ["dd"], "dd": ["d"], "ll": ["l"], "n": ["nn"], "nn": ["n"],
        "m": ["mm"], "mm": ["m"], "s": ["ss"], "ss": ["s"], "t": ["tt"],
        "tt": ["t"], "x": ["ks", "kz", "gz", "gs"], "ks": ["x"], "kz": ["x"],
        "gz": ["x"], "gs": ["x"], "qu": ["kw", "kv"], "q": ["c"], "kw": ["qu"],
        "ee": ["i"], "ea": ["i"], "ts": ["c"], "ew": ["ju"], "w": ["v"],
        "v": ["w"], "u": ["yu", "oo", "ju"], "oo": ["u"], "ju": ["u"], "ng": ["n"]
    }

    def generate(self, text, is_simple):
        all_variants = []

        if is_simple:
            for offset in range(1, 4):
                for i in range(len(text) - offset + 1):
                    if text[i:i + offset] in self.arr_rules:
                        for value in self.arr_rules[text[i:i + offset]]:
                            all_variants.append(text[:i] + value + text[i + offset:])
        else:
            variants = [[x] for x in list(text)]
            for offset in range(1, 4):
                for i in range(len(text) - offset + 1):
                    if text[i:i + offset] in self.arr_rules:
                        variants = variants[:i] + [["".join(p) for p in itertools.product(*variants[i:i + offset])] +
                                                   self.arr_rules[text[i:i + offset]]] + variants[i + offset:]
            all_variants = ["".join(p) for p in itertools.product(*variants)]
        
        return set(all_variants)


class EnhancedPhoneticsGenerator:
    def __init__(self):
        self.arr_rules = PhoneticsGenerator.arr_rules
        
        # Pre-compute max key length for optimization
        self.max_key_length = max(len(key) for key in self.arr_rules)
        
        # Index rules by first character for faster lookup
        self.indexed_rules = defaultdict(list)
        for key in self.arr_rules:
            if key:
                self.indexed_rules[key[0]].append(key)

    def generate_simple(self, text):
        """Generate variants using simple substitution approach."""
        result = set()
        
        # Only consider substitutions up to max key length
        for i in range(len(text)):
            # Only check keys that start with this character
            if text[i] not in self.indexed_rules:
                continue
                
            for key in self.indexed_rules[text[i]]:
                end = i + len(key)
                if end <= len(text) and text[i:end] == key:
                    for replacement in self.arr_rules[key]:
                        result.add(text[:i] + replacement + text[end:])
        
        # Add the original text if no variants were found
        if not result and text:
            result.add(text)
            
        return result

    def generate_complex(self, text):
        """Generate variants using a more thorough combinatorial approach."""
        # Identify all possible substitutions for each position
        variants = []
        for i in range(len(text)):
            char_variants = [text[i]]  # Start with the original character
            
            # Check all possible keys starting with this character
            if text[i] in self.indexed_rules:
                for key in self.indexed_rules[text[i]]:
                    end = i + len(key)
                    if end <= len(text) and text[i:end] == key:
                        # Add all possible replacements for this key
                        for replacement in self.arr_rules[key]:
                            # Skip multi-character replacements for now - they need special handling
                            if len(replacement) == 1 and replacement not in char_variants:
                                char_variants.append(replacement)
            
            variants.append(char_variants)
            
        # Handle multi-character replacements
        # This is a simplified approach that doesn't handle all complex cases
        # but is more efficient than the original algorithm
        result = set(["".join(p) for p in itertools.product(*variants)])
        
        # Now handle multi-character replacements
        additional_variants = set()
        for i in range(len(text)):
            if text[i] in self.indexed_rules:
                for key in self.indexed_rules[text[i]]:
                    end = i + len(key)
                    if end <= len(text) and text[i:end] == key and len(key) > 1:
                        for replacement in self.arr_rules[key]:
                            for variant in result:
                                if i < len(variant) and variant[i:i+len(key)] == key:
                                    new_variant = variant[:i] + replacement + variant[i+len(key):]
                                    additional_variants.add(new_variant)
        
        result.update(additional_variants)
        return result

    def generate(self, text, is_simple):
        """Generate phonetic variants of the input text."""
        if is_simple:
            return self.generate_simple(text)
        else:
            return self.generate_complex(text)


def benchmark(test_strings, repeats=1):
    generator1 = PhoneticsGenerator()
    generator2 = EnhancedPhoneticsGenerator()

    print(f"Benchmarking performance with {repeats} repeats...\n")

    results = {
        "Original": {"total_time": 0, "times": []},
        "Enhanced": {"total_time": 0, "times": []}
    }

    for r in range(repeats):
        for word in test_strings:
            # Test Original
            start_time = time.time()
            result1 = generator1.generate(word, False)
            original_time = time.time() - start_time
            results["Original"]["total_time"] += original_time
            results["Original"]["times"].append(original_time)

            # Test Enhanced
            start_time = time.time()
            result2 = generator2.generate(word, False)
            enhanced_time = time.time() - start_time
            results["Enhanced"]["total_time"] += enhanced_time
            results["Enhanced"]["times"].append(enhanced_time)

            if r == 0:  # Only print detailed comparison for first run
                print(f"Word: {word}")
                print(f"Original Time: {original_time:.6f} sec - {len(result1)} variants")
                print(f"Enhanced Time: {enhanced_time:.6f} sec - {len(result2)} variants")
                print("-" * 40)

    print("\nSummary:")
    print(f"Total Original Time: {results['Original']['total_time']:.6f} sec")
    print(f"Total Enhanced Time: {results['Enhanced']['total_time']:.6f} sec")
    
    # Calculate speedup
    original_time = results["Original"]["total_time"]
    enhanced_time = results["Enhanced"]["total_time"]
    
    print(f"\nSpeedup (Enhanced vs Original): {original_time/enhanced_time:.2f}x")


def verify_outputs(test_strings):
    generator1 = PhoneticsGenerator()
    generator2 = EnhancedPhoneticsGenerator()

    print("\nVerifying outputs...\n")
    
    matches = 0
    total = len(test_strings)

    for word in test_strings:
        output1 = set(generator1.generate(word, False))
        output2 = set(generator2.generate(word, False))

        if output1 == output2:
            print(f"✅ Outputs match for '{word}' - {len(output1)} variants")
            matches += 1
        else:
            print(f"❌ Mismatch for '{word}'")
            
            # Show output sizes
            print(f"  Original: {len(output1)} variants")
            print(f"  Enhanced: {len(output2)} variants")
            
            # Analyze the differences
            only_in_1 = output1 - output2
            only_in_2 = output2 - output1
            
            if only_in_1:
                print(f"\n  Only in Original ({len(only_in_1)} items):")
                print("  " + str(list(only_in_1)[:5]) + ("..." if len(only_in_1) > 5 else ""))
            
            if only_in_2:
                print(f"\n  Only in Enhanced ({len(only_in_2)} items):")
                print("  " + str(list(only_in_2)[:5]) + ("..." if len(only_in_2) > 5 else ""))
            
            print("-" * 40)

    # Summary
    print("\nSummary:")
    print(f"Total words tested: {total}")
    print(f"Implementations match: {matches} ({matches/total*100:.1f}%)")


if __name__ == "__main__":
    test_strings = [
        "nightt", "hello", "phonetix", "quick", "success", "bottle",  # Original ones
        "happy", "jumbo", "tough", "cycle", "knight", "physics",  # Common tricky words
        "bubble", "tunnel", "sizzle", "giraffe", "dizzy", "sphinx",  # Repeating letters, phonetic shifts
        "quizz", "check", "who", "whale", "rhythm", "xylophone",  # Edge cases with silent letters
        "llama", "ketchup", "yacht", "tsunami", "queue", "psychology",  # Words with unusual phonetics
        "khaki", "fjord", "mnemonic", "pneumonia", "czar", "gnarl",  # Rare/uncommon sounds
        "beef", "chief", "seize", "bizarre", "rogue", "debt",  # Silent and confusing letters
        "yummy", "jigsaw", "espresso", "adjourn", "luxury", "azure",  # Uncommon letter combinations
        "schooner", "knapsack", "cough", "enough", "though", "through"  # GH sound variations
    ]
    
    # Uncomment to run benchmark with multiple repeats (time-consuming)
    # benchmark(test_strings, repeats=10)
    
    # For quick test:
    benchmark(test_strings, repeats=1)
    verify_outputs(test_strings)