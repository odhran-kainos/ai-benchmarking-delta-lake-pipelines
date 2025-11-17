# Scorecard Template

This is a template for manually scoring AI-generated implementations.
Fill in the `<FILL_IN>` placeholders after reviewing the code and automated metrics.

## Scoring Scale (1-5)

- **5 - Excellent**: Exceeds expectations, production-ready
- **4 - Good**: Meets requirements with minor improvements needed  
- **3 - Acceptable**: Works but has notable issues
- **2 - Poor**: Significant problems, major rework needed
- **1 - Inadequate**: Fails basic requirements

## How to Complete This Scorecard

1. **Review automated metrics** in the `automated_metrics` section
2. **Read the diff report** to understand code changes
3. **Check the implementation** against task requirements
4. **Consult `benchmark/scoring_rubric.yaml`** for detailed criteria
5. **Fill in scores** (1-5) for each dimension
6. **Write rationale** explaining your scoring
7. **Calculate total score**: `(score1*weight1 + ... + score8*weight8) * 20`

## Dimension Weights (from benchmark/scoring.yaml)

- Correctness: 25%
- Maintainability: 15%
- Data Quality: 15%
- Planning: 15%
- Performance: 10%
- Documentation: 10%
- Security: 5%
- Productivity: 5%

## Example Calculation

If scores are [4, 4, 4, 4, 3, 4, 5, 4]:
```
total = (4×0.25 + 4×0.15 + 4×0.15 + 4×0.15 + 3×0.10 + 4×0.10 + 5×0.05 + 4×0.05) × 20
      = (1.0 + 0.6 + 0.6 + 0.6 + 0.3 + 0.4 + 0.25 + 0.2) × 20
      = 3.95 × 20
      = 79.0 / 100
```

## Reference

See `benchmark/scoring_rubric.yaml` for detailed 5-point rubrics for each dimension with:
- Specific criteria for each score level
- Examples of excellent, good, acceptable, poor, and inadequate implementations
- Automated checks to consider
- Manual review guidelines
