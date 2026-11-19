# swaggerui

Swagger UI served at `/docs`, embedded in the binary so the page works offline (ADR-0006 section 1, D38).

- Package: `swagger-ui-dist` 5.35.0 (Apache-2.0; `dist/LICENSE`, `dist/NOTICE`), fetched 2026-11-19 from https://registry.npmjs.org/swagger-ui-dist/-/swagger-ui-dist-5.35.0.tgz (npm shasum `5c3b487ae5a2121b2c668d6f92b550707521876a`).
- SHA-256 of the vendored files (checked by `TestVendoredFilesUnchanged`):

```
cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30  dist/LICENSE
0d20d1adef18aee3f40dd258172155521ce702ac445cb5f7b7d60ed32dad2fb2  dist/NOTICE
3ed612f41e050ca5e7000cad6f1cbe7e7da39f65fca99c02e99e6591056e5837  dist/favicon-32x32.png
e6a898692f2d768e807c3ba4626068a69a2c5dd960dd6c76f3e4d3a3de180af2  dist/swagger-ui-bundle.js
5ed08757e53da3e026d6cbecc61fe5d43585c9560b302d534ad56692519a273d  dist/swagger-ui.css
```

`dist/index.html` and `dist/swagger-initializer.js` are ours. `LICENSE` and `NOTICE` are embedded but not served.

To update: download the new tarball, copy the same five files unchanged into `dist/`, put the new SHA-256 values in the table of `swaggerui_test.go` and in this file, and update the version, URL and date above. Then run `go test ./internal/api/...`.
