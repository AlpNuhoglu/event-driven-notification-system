---
name: fable-orchestration
description: "Delegation policy for a multi-model agent stack where the main loop runs on a strong but expensive model (e.g. Claude Fable 5.2). The main loop orchestrates, writes briefs and synthesizes; the work is routed by difficulty. Medium-high difficulty goes to Opus with a fairly loose brief; simple-medium goes to Sonnet with a tighter brief; second opinions go to Opus with a peer-question brief. Very large data cleanup goes to Claude Haiku 5.5. Load whenever spawning sub-agents (Agent tool, Workflow agent() calls) or planning any delegation."
---

# Orchestration & delegation policy (expensive-main-loop stack)

A routing policy for stacks with **one strong, expensive main-loop model** and
**cheaper delegate tiers**. It is written for a Claude Fable 5.2 main loop with
Opus, Sonnet and Haiku sub-agents, but the shape transfers to any stack with the
same economics.

Core law: **the expensive model's tokens buy judgment, not throughput.** It
writes the plan, the briefs, the hard calls and the final synthesis.
Everything else goes to the cheapest tier that can carry it.

## The routing table

| Work | Goes to | How to call it | Brief style |
|---|---|---|---|
| Architecture, specs, hard judgment, integration, final synthesis | **Main loop** (Fable) | itself | n/a |
| Medium-high difficulty: multi-file implementation, real debugging, research that needs judgment, review and verification | **Opus** | `model: 'opus'` | Fairly loose: goal, context, constraints, what done looks like |
| Second opinion, out-of-the-box thinking, an angle the main loop may be missing | **Opus** | `model: 'opus'` | The open question plus the evidence, as to a peer |
| Simple-medium: scoped edits, pattern clones, lookups, templated work, straightforward reading and summarizing | **Sonnet** | `model: 'sonnet'` | A notch tighter: named files, steps, output shape, boundaries |
| Very large data cleanup: thousands of rows or files, normalizing, deduplicating, classifying, extracting | **Claude Haiku 5.5** | `model: 'haiku'` | Exact rule set with input and output examples, small batches, spot checks |

## The hard rules

1. **Always pass an explicit `model:`** on every Agent tool call, Workflow
   `agent()` call and `meta.phases` entry. Omitting it inherits the main-loop
   model, which silently fans the work out to your most expensive tier.
2. **The main-loop model is not a sub-agent tier.** Spawn it as a sub-agent
   only when the operator names it for that task, or when a rare piece that
   needs heavy judgment genuinely fails on Opus.
3. **Route by difficulty, then pick the cheapest tier that can carry it.**
   When unsure between two tiers, take the cheaper one and verify the result.
   If it fails, escalate one tier rather than starting high.
4. **The main loop keeps its own context lean.** Wide reading, file dumps and
   raw data go to a delegate that returns the conclusion; the main loop reads
   only what it must judge. Small things (a handful of tool calls, one known
   file) it does itself, because there a delegation costs more than it saves.
5. **A delegate's report is a claim.** Check the diff, the output or a sample
   before reporting the work as done.

## Brief shape per tier

How tightly to write a brief depends on who reads it.

- **Opus, for execution (fairly loose).** State:
  - the goal and why it matters;
  - the context it cannot discover on its own;
  - the constraints;
  - what done looks like.

  Leave the route to Opus. Over-specifying an Opus brief costs tokens and removes the judgment you are paying for.
- **Opus, for a second opinion (peer question).** Give it:
  - the problem and the evidence;
  - the options you see;
  - where you are unsure.

  Ask what you may be missing. **Do not hand it your conclusion to confirm.** Spawn it fresh, without your reasoning in its context, so that it is not anchored to your view. Treat the answer as a peer review, and disagree on evidence.
- **Sonnet (a notch tighter).**
  - Name the files or the area.
  - List the steps in order when order matters.
  - Give the output format.
  - Say what not to touch.
  - Say when to stop and report instead of improvising.
  - One task per agent.
- **Haiku 5.5 (bulk).**
  - Give it one mechanical rule set, stated exactly, with two or three input and output examples.
  - Split the data into batches small enough that a bad batch is cheap to redo.
  - Have each batch write to its own output file, and sample the results before merging.
  - Never ask this tier to decide what the rules should be.

Long briefs go in a file; the prompt points at the file.

## Exceptions

- If the operator explicitly names a model for a scoped task, honour it for
  that task only, then return to this policy.
- The operator can override any of this per session; otherwise this policy
  stands.
