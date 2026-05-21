# ARCHITECTURE.md

> Replace this template with the actual architecture of your project. The
> shape below is a suggestion — adapt to your stack.

## Overview

`<one paragraph>`. What the system does at a high level, the major
components, and how they fit together.

## Component layers

`<one diagram and a paragraph per layer>`. Use ASCII or Mermaid.

```
+--------------------+
| <entry point>      |
+--------------------+
         |
         v
+--------------------+
| <core logic>       |
+--------------------+
         |
         v
+--------------------+
| <data / I/O>       |
+--------------------+
```

## Key interfaces

`<for each interface boundary the system has, name it, name its contract, and
where it lives in the code>`.

| Interface | Contract | Code |
|---|---|---|
| `<example>` | `<one-line description>` | `<src/path/to/file.ext>` |

## Data flow

`<paragraph or numbered list>`. What flows through the system, in what shape,
and where it's transformed.

## What this document is NOT

- Not a tutorial. Tutorials live in `docs/` files named for the thing they
  teach.
- Not a roadmap. See `docs/ROADMAP.md`.
- Not a sales pitch. See `README.md`.
