import test from "node:test";
import assert from "node:assert/strict";
import { chapters, prepareMarkdown } from "./prepare-docs.mts";

test("both languages retain code and resolve repository links for the website", () => {
	const source =
		"# Guide\n\n[English](queries.en-US.md) · [缓存](cache.zh-CN.md)\n\n[Cache](cache.en-US.md#expiry)\n[Test](../e2e/query_lifecycle_test.go)\n[Example](../examples/bubbletea/)\n```go\ncacheq.Get[User](client, key)\n```\n";
	for (const locale of ["zh-CN", "en"]) {
		const result = prepareMarkdown(source, chapters[1], locale, 2, "main");
		assert.match(result, /title: "Guide"/);
		assert.ok(!result.includes("# Guide"));
		assert.ok(!result.includes("[English]"));
		assert.ok(
			result.includes(`[Cache](/cacheq/${locale === "en" ? "" : "zh-CN/"}docs/cache/#expiry)`),
		);
		assert.match(
			result,
			/https:\/\/github.com\/Waterkyuu\/cacheq\/blob\/main\/e2e\/query_lifecycle_test.go/,
		);
		assert.ok(result.includes("cacheq.Get[User](client, key)"));
		assert.ok(
			result.includes(
				"[Example](https://github.com/Waterkyuu/cacheq/tree/main/examples/bubbletea/)",
			),
		);
	}
});

test("edit links point to maintained documentation rather than generated content", () => {
	assert.ok(
		prepareMarkdown("# Start", chapters[0], "en", 1, "main").includes(
			"docs/getting-started.en-US.md",
		),
	);
	assert.ok(
		prepareMarkdown("# Cache", chapters[2], "zh-CN", 3, "main").includes("docs/cache.zh-CN.md"),
	);
});
