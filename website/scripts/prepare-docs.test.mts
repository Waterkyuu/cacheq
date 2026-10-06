import test from "node:test";
import assert from "node:assert/strict";
import { chapters, prepareMarkdown } from "./prepare-docs.mts";

test("both languages retain code and resolve repository links for the website", () => {
	const source =
		"# Guide\n\n[English](queries.en-US.md) · [缓存](cache.zh-CN.md)\n\n[Cache](cache.en-US.md#expiry)\n[Test](../e2e/query_lifecycle_test.go)\n```go\ncacheq.Get[User](client, key)\n```\n";
	for (const locale of ["zh-CN", "en"]) {
		const result = prepareMarkdown(source, chapters[1], locale, 2, "main");
		assert.match(result, /id: queries/);
		assert.ok(!result.includes("[English]"));
		assert.match(result, /\[Cache\]\(cache.md#expiry\)/);
		assert.match(
			result,
			/https:\/\/github.com\/Waterkyuu\/cacheq\/blob\/main\/e2e\/query_lifecycle_test.go/,
		);
		assert.ok(result.includes("cacheq.Get[User](client, key)"));
	}
});

test("only the introduction owns the root route", () => {
	assert.match(prepareMarkdown("# Start", chapters[0], "en", 1, "main"), /slug: \//);
	assert.ok(!prepareMarkdown("# Cache", chapters[2], "en", 3, "main").includes("slug:"));
});
