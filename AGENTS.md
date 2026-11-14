## Documentation

- Add only requirements stated by the user or suggestions the user has acknowledged.
- Keep unacknowledged suggestions out of the docs, including proposed rules and open decisions.
- Raise crucial unacknowledged suggestions in the response after editing the docs.
- Give readers the prerequisites, steps, and expected results for their task.
- Verify commands, examples, defaults, and links. Flag gaps and use placeholders for secrets.
- Update the canonical docs when behavior changes. Link to them from related pages.

## Design and implementation specs

- State the problem, scope, and constraints.
- Describe what changes, which components own it, which interfaces it affects, and why the design was chosen.
- Document dependencies, compatibility, failure handling, rollout, and rollback as needed. Flag open decisions.
- Sentences should be clear, concise, complete and conversational. 
- Documentation sentences must have explicit subjects that identify the responsible component or actor. Bullet points and numbered steps may omit the subject when the actor is clear from context.

## Agent rules

- Prefer IntelliJ tools for editing files when available.
- Use established project terminology consistently across documentation and code.
- Write one actionable directive per bullet. Include conditions and exceptions when they affect the action.
- Define shared rules once. Scope local rules narrowly and state any overrides.
- Keep context essential to the rule. Link to longer procedures and examples.
