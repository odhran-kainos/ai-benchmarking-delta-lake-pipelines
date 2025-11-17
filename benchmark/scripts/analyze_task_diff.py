#!/usr/bin/env python3
"""
Analyze Task Diff - Generate evaluation artifacts for AI tool benchmarking.

This script compares a baseline branch with an AI-generated implementation,
runs automated tests and metrics collection, and produces evaluation artifacts.

Usage:
    python benchmark/scripts/analyze_task_diff.py \\
        --baseline benchmark-foundation \\
        --implementation tool/copilot/task-T1 \\
        --output evaluations/copilot/T1

Outputs:
    - <output>_scorecard.yaml     - Manual scoring template with automated metrics
    - <output>_diff_report.md      - Human-readable diff analysis
    - <output>_metrics.json        - Raw metrics for programmatic use
"""

import argparse
import json
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
import yaml


class DiffAnalyzer:
    """Analyzes git diffs between baseline and implementation branches."""
    
    def __init__(self, baseline: str, implementation: str, repo_path: Path):
        self.baseline = baseline
        self.implementation = implementation
        self.repo_path = repo_path
    
    def get_diff_stats(self) -> Dict[str, Any]:
        """Get git diff statistics between branches."""
        try:
            # Get diff stats
            result = subprocess.run(
                ["git", "diff", "--shortstat", f"{self.baseline}..{self.implementation}"],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Parse output like: "5 files changed, 123 insertions(+), 45 deletions(-)"
            stats = {"files_changed": 0, "insertions": 0, "deletions": 0}
            
            if result.stdout.strip():
                parts = result.stdout.strip().split(", ")
                for part in parts:
                    if "file" in part:
                        stats["files_changed"] = int(part.split()[0])
                    elif "insertion" in part:
                        stats["insertions"] = int(part.split()[0])
                    elif "deletion" in part:
                        stats["deletions"] = int(part.split()[0])
            
            # Get list of changed files
            result = subprocess.run(
                ["git", "diff", "--name-only", f"{self.baseline}..{self.implementation}"],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                check=True
            )
            
            stats["changed_files"] = [f.strip() for f in result.stdout.split("\n") if f.strip()]
            
            return stats
        
        except subprocess.CalledProcessError as e:
            print(f"Error getting diff stats: {e}")
            return {"files_changed": 0, "insertions": 0, "deletions": 0, "changed_files": []}
    
    def get_diff_content(self) -> str:
        """Get the full diff content."""
        try:
            result = subprocess.run(
                ["git", "diff", f"{self.baseline}..{self.implementation}"],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            print(f"Error getting diff content: {e}")
            return ""
    
    def categorize_changes(self, changed_files: List[str]) -> Dict[str, List[str]]:
        """Categorize changed files by type."""
        categories = {
            "pipeline_code": [],
            "tests": [],
            "config": [],
            "documentation": [],
            "other": []
        }
        
        for file in changed_files:
            if file.startswith("pipelines/") and file.endswith(".py"):
                categories["pipeline_code"].append(file)
            elif file.startswith("tests/"):
                categories["tests"].append(file)
            elif file.endswith((".yaml", ".yml", ".json", ".conf")):
                categories["config"].append(file)
            elif file.endswith((".md", ".rst", ".txt")):
                categories["documentation"].append(file)
            else:
                categories["other"].append(file)
        
        return categories


class TestRunner:
    """Runs pytest tests and collects coverage metrics."""
    
    def __init__(self, repo_path: Path):
        self.repo_path = repo_path
    
    def run_tests(self, test_file: Optional[str] = None) -> Dict[str, Any]:
        """Run pytest tests with coverage."""
        test_target = test_file if test_file else "tests/"
        
        cmd = [
            "python", "-m", "pytest",
            test_target,
            "--cov=pipelines",
            "--cov-report=json",
            "--cov-report=term",
            "-v",
            "--tb=short"
        ]
        
        try:
            result = subprocess.run(
                cmd,
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            # Parse pytest output for test counts
            metrics = {
                "tests_run": 0,
                "tests_passed": 0,
                "tests_failed": 0,
                "tests_skipped": 0,
                "coverage_percent": 0.0,
                "exit_code": result.returncode,
                "output": result.stdout,
                "errors": result.stderr
            }
            
            # Try to parse coverage.json if it exists
            coverage_file = self.repo_path / "coverage.json"
            if coverage_file.exists():
                with open(coverage_file) as f:
                    coverage_data = json.load(f)
                    metrics["coverage_percent"] = coverage_data.get("totals", {}).get("percent_covered", 0.0)
            
            # Parse test counts from output
            for line in result.stdout.split("\n"):
                if "passed" in line or "failed" in line or "skipped" in line:
                    # Look for patterns like "5 passed, 2 failed, 1 skipped in 1.23s"
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if part == "passed" and i > 0:
                            metrics["tests_passed"] = int(parts[i-1])
                        elif part == "failed" and i > 0:
                            metrics["tests_failed"] = int(parts[i-1])
                        elif part == "skipped" and i > 0:
                            metrics["tests_skipped"] = int(parts[i-1])
            
            metrics["tests_run"] = metrics["tests_passed"] + metrics["tests_failed"] + metrics["tests_skipped"]
            
            return metrics
        
        except subprocess.TimeoutExpired:
            return {
                "tests_run": 0,
                "tests_passed": 0,
                "tests_failed": 0,
                "tests_skipped": 0,
                "coverage_percent": 0.0,
                "exit_code": -1,
                "output": "",
                "errors": "Test execution timeout (>5 minutes)"
            }
        except Exception as e:
            return {
                "tests_run": 0,
                "tests_passed": 0,
                "tests_failed": 0,
                "tests_skipped": 0,
                "coverage_percent": 0.0,
                "exit_code": -1,
                "output": "",
                "errors": str(e)
            }


class CodeQualityAnalyzer:
    """Analyzes code quality using pylint and other tools."""
    
    def __init__(self, repo_path: Path):
        self.repo_path = repo_path
    
    def run_pylint(self, files: List[str]) -> Dict[str, Any]:
        """Run pylint on specified files."""
        if not files:
            return {"score": 0.0, "output": "No files to analyze"}
        
        python_files = [f for f in files if f.endswith(".py")]
        if not python_files:
            return {"score": 0.0, "output": "No Python files to analyze"}
        
        try:
            result = subprocess.run(
                ["pylint"] + python_files + ["--output-format=json"],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                timeout=120
            )
            
            # Pylint exits with non-zero for issues, which is expected
            # Try to parse JSON output
            try:
                issues = json.loads(result.stdout)
                
                # Count by severity
                counts = {"error": 0, "warning": 0, "convention": 0, "refactor": 0}
                for issue in issues:
                    issue_type = issue.get("type", "").lower()
                    if issue_type in counts:
                        counts[issue_type] += 1
                
                # Extract score from stderr (pylint puts score there)
                score = 0.0
                for line in result.stderr.split("\n"):
                    if "Your code has been rated at" in line:
                        # Line like: "Your code has been rated at 8.50/10"
                        parts = line.split()
                        for i, part in enumerate(parts):
                            if part == "at" and i + 1 < len(parts):
                                score_str = parts[i + 1].split("/")[0]
                                score = float(score_str)
                                break
                
                return {
                    "score": score,
                    "issues": counts,
                    "total_issues": sum(counts.values()),
                    "files_analyzed": len(python_files)
                }
            
            except json.JSONDecodeError:
                # Fallback if JSON parsing fails
                return {
                    "score": 0.0,
                    "output": result.stdout,
                    "error": "Could not parse pylint output"
                }
        
        except subprocess.TimeoutExpired:
            return {"score": 0.0, "error": "Pylint timeout"}
        except FileNotFoundError:
            return {"score": 0.0, "error": "Pylint not installed"}
        except Exception as e:
            return {"score": 0.0, "error": str(e)}
    
    def check_secrets(self, files: List[str]) -> Dict[str, Any]:
        """Basic check for hardcoded secrets."""
        secrets_found = []
        
        patterns = [
            "password=",
            "api_key=",
            "secret=",
            "token=",
            "AWS_SECRET",
            "AZURE_CLIENT_SECRET"
        ]
        
        for file_path in files:
            full_path = self.repo_path / file_path
            if not full_path.exists() or not full_path.is_file():
                continue
            
            try:
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line_num, line in enumerate(f, 1):
                        for pattern in patterns:
                            if pattern.lower() in line.lower() and not line.strip().startswith("#"):
                                secrets_found.append({
                                    "file": file_path,
                                    "line": line_num,
                                    "pattern": pattern
                                })
            except Exception:
                continue
        
        return {
            "secrets_found": len(secrets_found),
            "details": secrets_found[:10]  # Limit to first 10
        }


class ReportGenerator:
    """Generates evaluation artifacts (scorecard, diff report, metrics)."""
    
    def __init__(self, output_path: str):
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
    
    def generate_scorecard(self, automated_metrics: Dict[str, Any], task_id: str) -> Path:
        """Generate scorecard YAML template with automated metrics filled in."""
        
        scorecard = {
            "task": task_id,
            "tool": "<FILL_IN>",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "evaluator": "<FILL_IN>",
            "automated_metrics": automated_metrics,
            "scores": {
                "correctness": {
                    "score": "<FILL_IN: 1-5>",
                    "rationale": "<Explain scoring based on test results>",
                    "auto_indicators": {
                        "tests_passed": f"{automated_metrics.get('test_results', {}).get('tests_passed', 0)}/"
                                      f"{automated_metrics.get('test_results', {}).get('tests_run', 0)}",
                        "coverage": f"{automated_metrics.get('test_results', {}).get('coverage_percent', 0):.1f}%"
                    }
                },
                "maintainability": {
                    "score": "<FILL_IN: 1-5>",
                    "rationale": "<Assess code quality, structure, readability>",
                    "auto_indicators": {
                        "pylint_score": automated_metrics.get('code_quality', {}).get('score', 0.0),
                        "total_issues": automated_metrics.get('code_quality', {}).get('total_issues', 0)
                    }
                },
                "data_quality": {
                    "score": "<FILL_IN: 1-5>",
                    "rationale": "<Evaluate validations, schema enforcement, quality checks>"
                },
                "planning": {
                    "score": "<FILL_IN: 1-5>",
                    "rationale": "<Assess architecture, design patterns, scalability>"
                },
                "performance": {
                    "score": "<FILL_IN: 1-5>",
                    "rationale": "<Evaluate execution efficiency, resource usage>"
                },
                "documentation": {
                    "score": "<FILL_IN: 1-5>",
                    "rationale": "<Review README, docstrings, comments, examples>"
                },
                "security": {
                    "score": "<FILL_IN: 1-5>",
                    "rationale": "<Check credential handling, input validation>",
                    "auto_indicators": {
                        "secrets_found": automated_metrics.get('security', {}).get('secrets_found', 0)
                    }
                },
                "productivity": {
                    "score": "<FILL_IN: 1-5>",
                    "rationale": "<Estimate time to completion, iterations needed>"
                }
            },
            "total_score": "<CALCULATE: weighted average * 20>",
            "notes": "<Additional observations, strengths, weaknesses>"
        }
        
        output_file = Path(str(self.output_path) + "_scorecard.yaml")
        with open(output_file, 'w') as f:
            yaml.dump(scorecard, f, default_flow_style=False, sort_keys=False)
        
        return output_file
    
    def generate_diff_report(self, diff_stats: Dict[str, Any], diff_content: str,
                           file_categories: Dict[str, List[str]]) -> Path:
        """Generate human-readable markdown diff report."""
        
        report = f"""# Diff Analysis Report

**Generated**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Change Summary

- **Files Changed**: {diff_stats.get('files_changed', 0)}
- **Lines Added**: {diff_stats.get('insertions', 0)}
- **Lines Deleted**: {diff_stats.get('deletions', 0)}
- **Net Change**: {diff_stats.get('insertions', 0) - diff_stats.get('deletions', 0)} lines

## Files by Category

### Pipeline Code ({len(file_categories.get('pipeline_code', []))})
"""
        for file in file_categories.get('pipeline_code', []):
            report += f"- `{file}`\n"
        
        report += f"\n### Tests ({len(file_categories.get('tests', []))})\n"
        for file in file_categories.get('tests', []):
            report += f"- `{file}`\n"
        
        report += f"\n### Configuration ({len(file_categories.get('config', []))})\n"
        for file in file_categories.get('config', []):
            report += f"- `{file}`\n"
        
        report += f"\n### Documentation ({len(file_categories.get('documentation', []))})\n"
        for file in file_categories.get('documentation', []):
            report += f"- `{file}`\n"
        
        if file_categories.get('other'):
            report += f"\n### Other ({len(file_categories.get('other', []))})\n"
            for file in file_categories.get('other', []):
                report += f"- `{file}`\n"
        
        report += "\n## Change Analysis\n\n"
        
        # Add insights based on changes
        pipeline_files = len(file_categories.get('pipeline_code', []))
        test_files = len(file_categories.get('tests', []))
        
        if pipeline_files == 0:
            report += "⚠️ **No pipeline code changes detected** - May not fulfill task requirements\n\n"
        
        if test_files == 0:
            report += "⚠️ **No test files added** - Implementation may lack test coverage\n\n"
        elif test_files > pipeline_files:
            report += "✅ **Good test coverage** - More test files than implementation files\n\n"
        
        if len(file_categories.get('documentation', [])) > 0:
            report += "✅ **Documentation added** - Good practice\n\n"
        
        report += "\n## Full Diff\n\n```diff\n"
        # Truncate diff if too long
        if len(diff_content) > 50000:
            report += diff_content[:50000]
            report += "\n\n... [Diff truncated - too large] ...\n"
        else:
            report += diff_content
        report += "\n```\n"
        
        output_file = Path(str(self.output_path) + "_diff_report.md")
        with open(output_file, 'w') as f:
            f.write(report)
        
        return output_file
    
    def generate_metrics_json(self, all_metrics: Dict[str, Any]) -> Path:
        """Generate JSON file with all metrics for programmatic use."""
        output_file = Path(str(self.output_path) + "_metrics.json")
        with open(output_file, 'w') as f:
            json.dump(all_metrics, f, indent=2)
        return output_file


def main():
    parser = argparse.ArgumentParser(
        description="Analyze diff between baseline and AI implementation"
    )
    parser.add_argument(
        "--baseline",
        required=True,
        help="Baseline branch (e.g., benchmark-foundation)"
    )
    parser.add_argument(
        "--implementation",
        required=True,
        help="Implementation branch/tag (e.g., tool/copilot/task-T1)"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output path prefix (e.g., evaluations/copilot/T1)"
    )
    parser.add_argument(
        "--task",
        help="Task ID for context (e.g., T1)",
        default="UNKNOWN"
    )
    parser.add_argument(
        "--repo-path",
        default=".",
        help="Path to git repository (default: current directory)"
    )
    
    args = parser.parse_args()
    
    repo_path = Path(args.repo_path).resolve()
    
    print(f"Analyzing diff: {args.baseline} -> {args.implementation}")
    print(f"Repository: {repo_path}")
    print(f"Output: {args.output}_*")
    print()
    
    # Initialize analyzers
    diff_analyzer = DiffAnalyzer(args.baseline, args.implementation, repo_path)
    test_runner = TestRunner(repo_path)
    quality_analyzer = CodeQualityAnalyzer(repo_path)
    report_generator = ReportGenerator(args.output)
    
    # Collect metrics
    print("📊 Collecting diff statistics...")
    diff_stats = diff_analyzer.get_diff_stats()
    diff_content = diff_analyzer.get_diff_content()
    file_categories = diff_analyzer.categorize_changes(diff_stats.get('changed_files', []))
    
    print(f"   - {diff_stats.get('files_changed', 0)} files changed")
    print(f"   - +{diff_stats.get('insertions', 0)} -{diff_stats.get('deletions', 0)} lines")
    print()
    
    print("🧪 Running tests...")
    test_results = test_runner.run_tests()
    print(f"   - {test_results['tests_passed']}/{test_results['tests_run']} passed")
    print(f"   - Coverage: {test_results['coverage_percent']:.1f}%")
    print()
    
    print("🔍 Analyzing code quality...")
    code_quality = quality_analyzer.run_pylint(file_categories.get('pipeline_code', []))
    print(f"   - Pylint score: {code_quality.get('score', 0.0)}/10")
    print()
    
    print("🔐 Checking for secrets...")
    security_check = quality_analyzer.check_secrets(diff_stats.get('changed_files', []))
    print(f"   - Potential secrets found: {security_check['secrets_found']}")
    print()
    
    # Aggregate all metrics
    all_metrics = {
        "timestamp": datetime.now().isoformat(),
        "baseline": args.baseline,
        "implementation": args.implementation,
        "task": args.task,
        "diff_stats": diff_stats,
        "file_categories": file_categories,
        "test_results": test_results,
        "code_quality": code_quality,
        "security": security_check
    }
    
    # Generate reports
    print("📝 Generating evaluation artifacts...")
    scorecard_file = report_generator.generate_scorecard(all_metrics, args.task)
    print(f"   ✅ {scorecard_file}")
    
    diff_report_file = report_generator.generate_diff_report(diff_stats, diff_content, file_categories)
    print(f"   ✅ {diff_report_file}")
    
    metrics_file = report_generator.generate_metrics_json(all_metrics)
    print(f"   ✅ {metrics_file}")
    
    print()
    print("✨ Analysis complete!")
    print()
    print("Next steps:")
    print(f"1. Review and complete the scorecard: {scorecard_file}")
    print(f"2. Read the diff report: {diff_report_file}")
    print(f"3. Calculate final score using weights from benchmark/scoring.yaml")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
