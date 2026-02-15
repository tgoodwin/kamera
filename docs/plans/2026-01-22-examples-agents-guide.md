# Examples AGENTS Guide Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an examples-level AGENTS guide that documents the headless dump workflow and determinize/cache notes across examples.

**Architecture:** Create `examples/AGENTS.md` with shared guidance extracted from the Knative guide: flags, headless workflow, determinize/cache notes, and a short debugging flow. Keep it general and avoid example-specific commands except for placeholders.

**Tech Stack:** Markdown documentation.

**Note:** Doc-only change; TDD not applicable.

### Task 1: Create examples-level AGENTS guide

**Files:**
- Create: `examples/AGENTS.md`

**Step 1: Draft AGENTS.md**

Add sections:
- Purpose (headless exploration + dump usage across examples)
- Common flags and their meanings
- Determinize/caches note (GOCACHE/GOMODCACHE + determinize script placeholder)
- Suggested headless workflow with placeholders
- Suggested debugging flow (log first, inspect dump before changing behavior)

**Step 2: Commit**

```bash
git add examples/AGENTS.md
git commit -m "add examples agent workflow guide"
```
