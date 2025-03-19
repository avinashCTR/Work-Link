import time
import itertools

class PhoneticsGeneratorRecursive:
    # Recursive implementation (your code)
    arr_rules = {
        "1": ["l", "I"],
        "l": ["1", "ll"],
        "I": ["1"],
        "6": ["b"],
        "b": ["6"],
        "0": ["O", "o"],
        "O": ["0"],
        "o": ["0", "ou"],
        "y": ["i", "j"],
        "i": ["y"],
        "c": ["ck"],
        "k": ["ck"],
        "f": ["ph"],
        "ph": ["f"],
        "g": ["dzh"],
        "dzh": ["g", "j"],
        "j": ["dzh"],
        "h": ["kh"],
        "kh": ["h"],
        "d": ["dd"],
        "dd": ["d"],
        "ll": ["l"],
        "n": ["nn"],
        "nn": ["n"],
        "m": ["mm"],
        "mm": ["m"],
        "s": ["ss"],
        "ss": ["s"],
        "t": ["tt"],
        "tt": ["t"],
        "x": ["ks", "kz", "gz", "gs"],
        "ks": ["x"],
        "kz": ["x"],
        "gz": ["x"],
        "gs": ["x"],
        "qu": ["kw", "kv"],
        "q": ["c"],
        "kw": ["qu"],
        "ee": ["i"],
        "ea": ["i"],
        "ts": ["c"],
        "ew": ["ju"],
        "w": ["v"],
        "v": ["w"],
        "u": ["yu", "oo", "ju"],
        "oo": ["u"],
        "ju": ["u"],
        "ng": ["n"]
    }


    def generate(self, text, is_simple):
        text = text.lower()
        all_variants = set()
        
        if is_simple:
            # Non-recursive fast path for simple mode
            for offset in range(1, 4):
                for i in range(len(text) - offset + 1):
                    key = text[i:i+offset]
                    if key in self.arr_rules:
                        for replacement in self.arr_rules[key]:
                            all_variants.add(text[:i] + replacement + text[i+offset:])
            return list(all_variants)
        else:
            # Fast recursive implementation with limited depth
            MAX_SUBSTITUTIONS = 2  # Reduced from unlimited
            text_list = list(text)
            
            def helper(index, subs_left, current):
                if index >= len(text_list):
                    all_variants.add(''.join(current))
                    return
                
                # Option 1: Make substitution (if allowed)
                if subs_left > 0:
                    for offset in range(1, 4):
                        if index + offset > len(text_list):
                            continue
                        key = ''.join(current[index:index+offset])
                        if key in self.arr_rules:
                            for replacement in self.arr_rules[key]:
                                new_current = current[:index] + list(replacement) + current[index+offset:]
                                helper(index + len(replacement), subs_left-1, new_current)
                
                # Option 2: Skip substitution
                helper(index + 1, subs_left, current)

            helper(0, MAX_SUBSTITUTIONS, text_list.copy())
            return list(all_variants)

class PhoneticsGenerator:
    arr_rules = {
        "1": ["l", "I"],
        "l": ["1", "ll"],
        "I": ["1"],
        "6": ["b"],
        "b": ["6"],
        "0": ["O", "o"],
        "O": ["0"],
        "o": ["0", "ou"],
        "y": ["i", "j"],
        "i": ["y"],
        "c": ["ck"],
        "k": ["ck"],
        "f": ["ph"],
        "ph": ["f"],
        "g": ["dzh"],
        "dzh": ["g", "j"],
        "j": ["dzh"],
        "h": ["kh"],
        "kh": ["h"],
        "d": ["dd"],
        "dd": ["d"],
        "ll": ["l"],
        "n": ["nn"],
        "nn": ["n"],
        "m": ["mm"],
        "mm": ["m"],
        "s": ["ss"],
        "ss": ["s"],
        "t": ["tt"],
        "tt": ["t"],
        "x": ["ks", "kz", "gz", "gs"],
        "ks": ["x"],
        "kz": ["x"],
        "gz": ["x"],
        "gs": ["x"],
        "qu": ["kw", "kv"],
        "q": ["c"],
        "kw": ["qu"],
        "ee": ["i"],
        "ea": ["i"],
        "ts": ["c"],
        "ew": ["ju"],
        "w": ["v"],
        "v": ["w"],
        "u": ["yu", "oo", "ju"],
        "oo": ["u"],
        "ju": ["u"],
        "ng": ["n"]}

    # @staticmethod
    # def apply_rule(gen, rule, src, offset):
    #     return src[:gen['pos']+offset] + \
    #            rule[1] + \
    #            src[gen['pos']+offset+len(rule[0]): len(src)]

    def generate(self, text, is_simple):
        all_variants = []
        # arr_gens = []  # Array for generations

        # Checks for presence of pattern in arr_rules starting at position i and adds it to the generator list
        text = text.lower()
        # for i in range(len(text)):
        #     for key in self.arr_rules:
        #         arr_rules_len = len(key)
        #         if key == text[i: i + arr_rules_len]:
        #             for rules in self.arr_rules[key]:
        #                 temp = {'z': 2, 'pos': i, 'rule': rules}
        #                 arr_gens.append(temp)
        #
        # n = len(arr_gens)

        # Generates only 1 rule variation
        if is_simple:  # Use simple algorithm
            # for i in range(n):
            #     all_variants.append(
            #         self.apply_rule(
            #             arr_gens[i], self.arr_rules[arr_gens[i]['rule']],
            #             text, 0
            #         )
            #     )
            for offset in range(1, 4):
                for i in range(len(text) - offset + 1):
                    if text[i:i + offset] in self.arr_rules:
                        for value in self.arr_rules[text[i:i + offset]]:
                            all_variants.append(text[:i] + value + text[i + offset:])

        else:  # Use COMBINE, more complex algorithm
            # for(f=0; f<Math.pow(2,n)-1; f++) #commented part /*&&
            # all_variants.length<6
            # for f in range(2 ** n - 1):
            #     i = 0  # Bit index
            #     var_str = text
            #     while arr_gens[i]['z'] == 1:
            #         arr_gens[i]['z'] = 0  # Modeling of next digit transfer
            #         # while adding
            #         log2 = log2[:i] + '0' + log2[i + 1:]
            #         i += 1
            #     arr_gens[i]['z'] = 1
            #     log2 = log2[:i] + '1' + log2[i + 1:]
            #     bits = log2.count("1")
            #     offset = 0
            #
            #     for t in range(n):
            #         # if 1 - apply rule
            #         if arr_gens[t]['z'] == 1 and arr_gens[t]['rule']:
            #             length = len(self.arr_rules[arr_gens[t]['rule']][1])
            #             var_str = self.apply_rule(
            #                 arr_gens[i],
            #                 self.arr_rules[arr_gens[t]['rule']],
            #                 var_str, offset
            #             )
            #             if length > 1 and bits > 1:
            #                 offset += length - 1
            #     all_variants.add(var_str)
            variants = [[x] for x in list(text)]
            for offset in range(1, 4):
                for i in range(len(text) - offset + 1):
                    if text[i:i + offset] in self.arr_rules:
                        variants = variants[:i] + [["".join(p) for p in itertools.product(*variants[i:i + offset])] +
                                                   self.arr_rules[text[i:i + offset]]] + variants[i + offset:]
            all_variants = ["".join(p) for p in itertools.product(*variants)]
        all_variants = list(all_variants)
        return all_variants


if __name__ == "__main__":
    test_cases = [
        "world",
        "hello", "world", "phonetics", "generator", "recursive", "simple", "complex",
        "test", "example", "python", "programming", "language", "rules", "variants",
        "comparison", "performance", "optimization", "algorithm", "implementation",
        "function", "method", "class", "object", "inheritance", "polymorphism",
        "encapsulation", "abstraction", "data", "structure", "array", "list", "tuple",
        "dictionary", "set", "string", "integer", "float", "boolean", "loop", "condition",
        "recursion", "iteration", "branching", "syntax", "semantics", "debugging",
        "testing", "validation", "verification", "execution", "compilation", "interpretation",
        "module", "package", "library", "framework", "tool", "utility", "application",
        "software", "hardware", "network", "protocol", "security", "encryption", "decryption",
        "authentication", "authorization", "database", "query", "index", "transaction",
        "backup", "restore", "cloud", "storage", "virtualization", "container", "orchestration",
        "deployment", "integration", "continuous", "delivery", "pipeline", "monitoring",
        "logging", "tracing", "metrics", "alerting", "scaling", "load", "balancing", "cache",
        "performance", "optimization", "latency", "throughput", "availability", "reliability",
        "resilience", "fault", "tolerance", "recovery", "disaster", "recovery", "business",
        "continuity", "compliance", "regulation", "governance", "risk", "management", "audit",
        "certification", "accreditation", "standardization", "framework", "methodology",
        "process", "procedure", "practice", "guideline", "recommendation", "requirement",
        "specification", "architecture", "design", "implementation", "testing", "deployment",
        "maintenance", "support", "documentation", "training", "knowledge", "transfer",
        "communication", "collaboration", "coordination", "cooperation", "teamwork", "leadership",
    ]

    total_recursive_time = 0
    total_original_time = 0

    for test_case in test_cases:
        print(f"Testing with input: {test_case}")
        start_time = time.time()
        recursive_variants = PhoneticsGeneratorRecursive().generate(test_case, is_simple=False)
        total_recursive_time += time.time() - start_time

        start_time = time.time()
        original_variants = PhoneticsGenerator().generate(test_case, is_simple=False)
        total_original_time += time.time() - start_time

    print(f"Total Recursive Time: {total_recursive_time:.6f} seconds")
    print(f"Total Original Time: {total_original_time:.6f} seconds")
    print(f"Speedup: {total_original_time / total_recursive_time:.2f}x")

    # Compare the results
    for test_case in test_cases:
        recursive_variants = PhoneticsGeneratorRecursive().generate(test_case, is_simple=False)
        original_variants = PhoneticsGenerator().generate(test_case, is_simple=False)
        if set(recursive_variants) != set(original_variants):
            print(f"the difference in set length for {test_case} is {len(set(recursive_variants)) - len(set(original_variants))}")
            # print(f"the set from recursive is {set(recursive_variants)}")
            # print(f"the set from original is {set(original_variants)}")