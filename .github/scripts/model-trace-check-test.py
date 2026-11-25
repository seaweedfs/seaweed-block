#!/usr/bin/env python3
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("model-trace-check.py")
spec = importlib.util.spec_from_file_location("model_trace", SCRIPT)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class ModelTraceTests(unittest.TestCase):
    mapping = {"core/a.go": {"Kernel.Begin", "Kernel.Commit"}, "core/b.go": {"Lease.Renew"}}

    def test_each_mapped_file_needs_its_own_citation(self):
        problems = checker.check("Kernel.Begin", ["core/a.go", "core/b.go"], self.mapping)
        self.assertEqual(len(problems), 1)
        self.assertIn("core/b.go", problems[0])
        self.assertEqual(checker.check("Kernel.Begin; Lease.Renew", list(self.mapping), self.mapping), [])

    def test_mapped_file_refuses_free_text_wrong_actions_and_lookalikes(self):
        for model in ("exhausted", "Kernel.Unknown", "Other.Begin", "Kernel.BeginExtra",
                      "docs/Kernel.Begin", "example: Kernel.Begin", "NONE: no state changes",
                      "NONE (pure function of Other)", "NONE (pure function of Kernel) extra"):
            with self.subTest(model=model):
                self.assertEqual(len(checker.check(model, ["core/a.go"], self.mapping)), 1)

    def test_pure_function_exception_is_module_specific(self):
        self.assertEqual(checker.check("NONE (pure function of Kernel)", ["core/a.go"], self.mapping), [])
        self.assertEqual(len(checker.check("NONE (pure function of Kernel)", ["core/b.go"], self.mapping)), 1)

    def test_unmapped_files_keep_existing_policy(self):
        self.assertEqual(checker.check("existing prose", ["docs/a.md", "core/a.go.bak"], self.mapping), [])

    def test_cli_checks_nul_paths_and_model_citations(self):
        with tempfile.TemporaryDirectory() as tmp:
            mapping = Path(tmp) / "map.tsv"
            mapping.write_text("# exact paths\ncore/a.go\tKernel.Begin\n", encoding="utf-8")
            def run(model, paths):
                return subprocess.run([sys.executable, str(SCRIPT), str(mapping), model],
                                      input=paths, capture_output=True).returncode
            self.assertEqual(run("Kernel.Begin", b"core/a.go\0"), 0)
            self.assertEqual(run("unstructured prose", b"core/a.go\0"), 1)
            self.assertEqual(run("Kernel.Begin", b"core/a.go"), 2)
            self.assertEqual(run("Kernel.Begin", b"\0"), 2)

    def test_mapping_refuses_missing_empty_and_malformed_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            mapping = Path(tmp) / "map.tsv"
            with self.assertRaises(OSError):
                checker.read_mapping(mapping)
            for content in ("# not populated\n", "core/a.go Kernel.Begin\n",
                            "../core/a.go\tKernel.Begin\n", "core//a.go\tKernel.Begin\n",
                            "core/a.go\tKernel\n", "core/a.go\tKernel.Begin extra\n"):
                with self.subTest(content=content):
                    mapping.write_text(content, encoding="utf-8")
                    with self.assertRaises(ValueError):
                        checker.read_mapping(mapping)


if __name__ == "__main__":
    unittest.main()
