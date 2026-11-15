## Documentation

- Include only user-stated or explicitly approved requirements. Do not turn assumptions into restrictions or add unapproved proposals and open decisions.
- Raise crucial unacknowledged suggestions in the response after editing the docs.
- Give readers the prerequisites, steps, and expected results for their task.
- Verify commands, examples, defaults, and links. Flag gaps and use placeholders for secrets.
- When the user requests documentation changes, update the canonical docs and link to them from related pages.

## Design and implementation specs

- State the problem, scope, and constraints.
- Describe what changes, which components own it, which interfaces it affects, and why the design was chosen.
- Document dependencies, compatibility, failure handling, rollout, and rollback as needed. Flag open decisions.
- Sentences should be clear, concise, complete and conversational.
- Follow the section's existing format. Keep additions in short paragraphs when the section uses prose instead of switching to bullet points.
- Documentation sentences must have explicit subjects that identify the responsible component or actor. Bullet points and numbered steps may omit the subject when the actor is clear from context.

## Agent rules

- Keep responses concise and apply the user's corrections consistently in later work.
- Delegate all code changes to the `implementer` agent.
- For code requests, do not edit documentation `.md` files.
- For docs/specs requests, do not edit code without a separate instruction.
- Report conflicts and await the user's final decision before editing.
- Prefer IntelliJ tools for editing files when available.
- Use established project terminology consistently across documentation and code.
- Write one actionable directive per bullet. Include conditions and exceptions when they affect the action.
- Define shared rules once. Scope local rules narrowly and state any overrides.
- Keep context essential to the rule. Link to longer procedures and examples.
