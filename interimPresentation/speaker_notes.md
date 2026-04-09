# Speaker Notes: Cloud Native Analytical Database Benchmarking
## Progress Update Presentation

**Total Time: 10 minutes**

---

## Slide 1: Title Slide
**Time: 0:00 - 0:10 (10 seconds)**

- "Good afternoon. I'll be presenting my progress on Cloud Native Analytical Database Benchmarking."

---

## Slide 2: Outline
**Time: 0:10 - 0:20 (10 seconds)**

- "Quick overview: a brief recap of the problem, then methodology, work completed, and future plans."

---

## Slide 3: Recap: Research Gap & Objectives
**Time: 0:20 - 0:50 (30 seconds)**

- "As you know, TPC benchmarks don't reflect real cloud workloads—production has 50%+ writes and heavy-tailed latencies."
- "Prior work addresses either hardware profiles OR query structure, but not both."
- "This thesis builds a unified benchmark matching both dimensions."
- "Three objectives: synthesis pipeline, cross-config dataset, and rigorous validation."

---

## Slide 4: Overall Approach
**Time: 0:50 - 1:20 (30 seconds)**

- "Here's the four-phase methodology."
- "Starting from production traces, I build a query pool, profile on different hardware, then match and schedule queries to reproduce trace characteristics."
- "The green line shows current progress—phases 1 and 2 complete."
- "Key techniques: multi-objective optimization—exploring Integer Linear Programming (ILP), simulated annealing, and LLM-based approaches. LLM augmentation inspired by recent SQLBarber and SQLStorm work. And learned performance prediction."

---

## Slide 5: Infrastructure Setup
**Time: 1:20 - 1:50 (30 seconds)**

- "What I've built: DuckDB-based analysis infrastructure for 500M+ records with memory-efficient views."
- "Both TPC-DS and TPC-H at Scale Factor 1—using official DSGen and DBGen tools."
- "For profiling, I'm running queries in lightweight Alpine Linux VMs with configurable CPU and RAM."
- "Currently focused on execution time as the primary metric—plan to add hardware counters like CPU cycles and cache misses in the next phase."

---

## Slide 6: Datasets Analyzed
**Time: 1:50 - 2:10 (20 seconds)**

- "Three datasets: Snowset with operator-level detail, Redset Serverless and Provisioned with query type and cache information."
- "All unified through DuckDB views over Parquet files."

---

## Slide 7: Key Findings from Trace Analysis (Recap)
**Time: 2:10 - 2:50 (40 seconds)**

- "Quick recap of findings you've seen before that drive the synthesis approach."
- "Left side: workload characteristics—heavy-tailed durations with 95th percentile (P95) 100x median, 50%+ writes, scan-dominated CPU."
- "Right side: novel discoveries—saturation threshold at 110 queries, non-monotonic warehouse scaling, low cache hits despite repetition."
- "These define our matching targets. The synthetic workload must reproduce these patterns."

---

## Slide 8: TPC-DS & TPC-H Baseline Profiling
**Time: 2:50 - 3:10 (20 seconds)**

- "I've profiled all 99 TPC-DS templates plus the 22 TPC-H templates."
- "Running in Alpine VMs with varying CPU and RAM—2 to 8 cores, 2 to 8 GB memory."
- "Currently capturing execution time; hardware counters like cache misses come next."
- "Confirmed gaps: only SELECT queries, inner-join bias, numeric keys."
- "The query pool needs substantial augmentation."

---

## Slide 9: LLM Augmentation: Preliminary Work
**Time: 3:10 - 3:45 (35 seconds)**

- "I've started preliminary LLM augmentation work."
- "Using TPC-H and TPC-DS queries as seeds—asking the LLM to add outer joins, text predicates, Common Table Expressions (CTEs) to existing templates."
- "Tested multiple OpenAI models. GPT-4o-mini is fast and cheap for bulk work, but struggles with schema consistency. GPT-4o produces higher quality but is slower and more expensive."
- "One approach I'm considering is cross-AI validation—use one model to generate queries, another to validate and critique."
- "This could catch hallucinated table names or invalid SQL before we waste time profiling bad queries."
- "Exploring Claude and Gemini as potential validation backends alongside OpenAI."

---

## Slide 10: Remaining Work: Phase 3 -- LLM Augmentation
**Time: 3:45 - 4:15 (30 seconds)**

- "Building on those preliminary experiments, Phase 3 will scale up with two complementary strategies."
- "SQLBarber-style: controlled generation with structural constraints and self-correction loops."
- "SQLStorm-style: large-scale stochastic generation with filtering."
- "Targets include writes, metadata queries, outer joins, text keys—the gaps we identified."
- "Multi-environment profiling across VM sizes and scale factors builds our performance models."

---

## Slide 11: Remaining Work: Matching Algorithm
**Time: 4:15 - 4:40 (25 seconds)**

- "The matching algorithm has three stages."
- "Database assignment and filtering, performance matching accounting for concurrency, structural balance for operator distributions."
- "For optimization, I'm exploring multiple approaches: ILP for discrete selection, simulated annealing for temporal dynamics, and potentially LLM-guided search for iterative refinement."
- "The choice depends on which achieves the best fidelity-to-computation tradeoff."

---

## Slide 12: Remaining Work: Phase 4 Validation
**Time: 4:40 - 5:00 (20 seconds)**

- "Four validation dimensions:"
- "Performance fidelity, structural fidelity, temporal dynamics, and cross-deployment generalization."
- "The last is critical—workloads must behave consistently across hardware configs."

---

## Slide 13: Success Metrics
**Time: 5:00 - 5:30 (30 seconds)**

- "Let me explain these metrics."
- "Kullback–Leibler (KL) divergence measures how different two probability distributions are—under 0.1 means the synthetic duration distribution closely matches the production trace."
- "Operator proportions within 10%—if production has 40% scans, we want 36-44% in our synthetic workload."
- "Concurrency within 15%—peak and median concurrent query counts should match the trace's temporal patterns."
- "Cross-deployment stability means if we profile on 3 different VM configs, the relative query rankings should be consistent."
- "Deliverables: open-source pipeline so others can reproduce, augmented query pool, performance profiles, and a validation dataset with comparative metrics."

---

## Slide 14: Timeline
**Time: 5:30 - 5:50 (20 seconds)**

- "Green is complete—trace analysis through profiling."
- "Yellow in progress—LLM augmentation."
- "Blue planned: multi-VM profiling in February, matching and validation February-March, writing through April."
- "Submission April 2026."

---

## Slide 15: Risk Mitigation
**Time: 5:50 - 6:10 (20 seconds)**

- "Key risks and mitigations."
- "LLM quality: validation pipeline with self-correction—plus cross-AI validation as discussed."
- "Performance prediction: fallback to learned models."
- "Costs: spot instances, smaller models for bulk work."
- "Alternative approaches ready if primary methods fall short."

---

## Slide 16: Summary
**Time: 6:10 - 6:25 (15 seconds)**

- "To summarize: analyzed 500M+ queries, quantified TPC gaps, built infrastructure, started LLM augmentation."
- "Next: scale up query generation, matching algorithm exploration, cross-config validation."
- "Goal: benchmark generator reflecting actual customer usage."

---

## Slide 17: Questions
**Time: 6:25 - 6:30 (5 seconds)**

- "Thank you. Happy to take questions."

---

## Backup Talking Points (if questions arise)

### On the saturation threshold:
- "Discovered through log-log regression. Below 110 queries, slope is -0.31 (sub-linear). Above, slope exceeds 1.05 (super-linear)."

### On SQLBarber vs SQLStorm:
- "SQLBarber is precision-focused: you specify constraints, get templates, then optimize predicate values with Bayesian methods. Great for hitting specific cost targets."
- "SQLStorm is diversity-focused: generate thousands of queries stochastically, filter invalid ones, use LLM rewriting for compatibility. Great for stress-testing and coverage."
- "I'll likely use a hybrid—SQLBarber for controlled augmentation of missing query types, SQLStorm for maximizing plan diversity."

### On LLM query generation:
- "GPT-4o-mini with temperature 0 for deterministic template generation. Each query validated by execution. Self-correction loop catches hallucinated table/column names."

### On why DuckDB:
- "Embedded engine querying Parquet directly. Essential for 500M+ records without impractical RAM."

### On the matching algorithm:
- "Still exploring optimization approaches. ILP gives optimal selection but may not scale. Simulated annealing handles temporal dynamics well. LLM-guided search could provide adaptive refinement—using the LLM to reason about which queries to add or remove based on current fidelity gaps."

### On cross-deployment validation:
- "If Query A takes 2x Query B on one machine, that ratio should hold on others. Test across 3+ VM configurations."
