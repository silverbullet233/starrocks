# STRING_V2 Implementation Loop

You are an autonomous agent executing the STRING_V2 implementation plan for StarRocks. Each iteration you complete ONE task, then exit. The loop will restart you automatically for the next task.

## State Files

- **Plan (source of truth):** `docs/superpowers/plans/2026-04-12-string-v2-implementation.md`
- **Design Spec:** `docs/superpowers/specs/2026-04-12-string-v2-type-design.md`
- **Branch:** `add_string_v2`

## Your Workflow (EVERY iteration)

### Step 1: Determine what to do

Read the plan file. Find the current state:

- Find the first task whose steps still have `- [ ]` (unchecked) checkboxes. That is your current task.
- If ALL checkboxes across ALL phases are `- [x]`, output `<promise>IMPLEMENTATION COMPLETE</promise>` and stop.

### Step 2: Read context

- Read the design spec section relevant to the current task.
- Read the specific task section from the plan file — it contains exact files, steps, and acceptance criteria.
- If the task references existing code patterns, read those files to understand the pattern before writing code.

### Step 3: Execute the task

- Implement all steps listed in the task.
- Follow the codebase conventions (read `CLAUDE.md` and `be/AGENTS.md` if this is BE work).
- For each step, verify it works (compile check, test run, etc.) before moving on.

### Step 4: Verify acceptance criteria

- Check every acceptance criterion listed in the task.
- If any criterion fails, debug and fix before proceeding.

### Step 5: Update the plan

- In the plan file, change all `- [ ]` to `- [x]` for the steps you completed in this task.
- This is critical — it is how the next iteration knows what to do.

### Step 6: Commit

Create a git commit:
```bash
git add -A
git commit -m "[Feature] STRING_V2: Task N.M — <brief description>"
```

### Step 7: Phase completion check

After completing the last task of a Phase (check if the next task is in a different Phase section or there are no more tasks):

1. Mark the Phase header as completed: add `-- COMPLETED` to the Phase heading.
2. Run a code review using the `superpowers:code-reviewer` subagent against the design spec.
3. If the review finds critical issues, create a fix commit before moving on.
4. Commit the plan update:
   ```bash
   git add docs/superpowers/plans/2026-04-12-string-v2-implementation.md
   git commit -m "[Doc] Update plan: mark Phase N as completed"
   ```

### Step 8: Exit

After completing ONE task (and optional Phase review), exit normally. The ralph loop will restart you for the next task.

## Important Rules

- **ONE task per iteration.** Do not attempt multiple tasks.
- **Always update the plan file** before committing. The plan file IS the state machine.
- **Read before write.** Always read existing code before modifying it.
- **Match existing patterns.** When adding new code, follow the conventions of nearby code.
- **Test what you can.** If there's a relevant UT binary, run it. If the FE has a compile check, run it.
- **Do not skip acceptance criteria.** Every criterion must be verified.
- **Commit message format:** `[Feature] STRING_V2: Task N.M — <description>`

## Build Commands Reference

```bash
# BE build
./build.sh --be -j 80

# FE build
./build.sh --fe -j 80

# Full build (for E2E testing, Phase 4+)
./build.sh --be --fe --enable-shared-data -j 80

# BE unit test (specific binary)
./run-be-ut.sh --build-target <test_binary> --module <test_binary> --without-java-ext

# FE compile check
cd fe && mvn compile -DskipTests -pl fe-core -am
```

## Cluster Deployment (Phase 4+ only)

```bash
# Stop/Start FE
cd output/fe && ./bin/stop_fe.sh
cd output/fe && ./bin/start_fe.sh --daemon

# Stop/Start BE
cd output/be && ./bin/stop_be.sh
cd output/be && ./bin/start_be.sh --daemon

# Access
mysql -h127.0.0.1 -P9030 -u root
```
