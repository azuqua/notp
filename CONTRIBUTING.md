
# Forth Eorlingas!

## Pull Requests

For most PR's (read: additive feature requests), please submit against the `develop` branch;
this is to ensure that we may quickly merge the changes in and allow the community to critique/modify
the proposed changes from a common source.

## Code Format

Follow the same coding format seen in the source code; the one hard requirement is that code indentation
**must** be two hard spaces (no soft tabs), this is to ensure that diff views of code submission remains legible.

## Tests

There is an exhaustive unit test suite under `/test`, which can be run using both `mocha test` or `grunt test`.

PR's that provide additional functionality should also provide corresponding unit test cases.

## Documentation

Anybody and everybody can help with documentation on this project, and there's a lot to be done. Specifically with documenting events and when they're fired/listened to, and also with examples of how to use different modules in this library. So if you find some topic confusing or lacking in documentation, please make a PR or an issue (and thanks in advance).
