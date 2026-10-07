import test from "node:test";
import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import path from "node:path";

interface SearchRecord {
	title: string;
	url: string;
	language: string;
	content: string;
}
const build = path.resolve("build");

async function htmlFiles(directory: string): Promise<string[]> {
	const files: string[] = [];
	for (const item of await readdir(directory, { withFileTypes: true })) {
		const file = path.join(directory, item.name);
		if (item.isDirectory()) files.push(...(await htmlFiles(file)));
		else if (item.name.endsWith(".html")) files.push(file);
	}
	return files;
}

async function assertDestination(destination: string, currentFile: string) {
	if (
		destination.startsWith("https://") ||
		destination.startsWith("http://") ||
		destination.startsWith("mailto:")
	)
		return;
	const url = new URL(
		destination,
		"https://waterkyuu.github.io/cacheq/" + path.relative(build, currentFile),
	);
	assert.ok(
		url.pathname.startsWith("/cacheq/"),
		`Link escaped the repository base: ${destination}`,
	);
	let relative = decodeURIComponent(url.pathname.slice("/cacheq/".length));
	if (relative.endsWith("/") || !relative) relative += "index.html";
	const content = await readFile(path.join(build, relative), "utf8");
	if (url.hash) {
		const anchor = decodeURIComponent(url.hash.slice(1));
		assert.ok(content.includes(`id="${anchor}"`), `Missing heading ${destination}`);
	}
}

test("every published page has server-rendered content and working internal links", async () => {
	const english = await readFile(path.join(build, "index.html"), "utf8");
	assert.match(english, /<html\b[^>]*lang="en"/);
	assert.ok(english.includes("Go queries, made simple."));
	const chinese = await readFile(path.join(build, "zh-CN/index.html"), "utf8");
	assert.match(chinese, /<html\b[^>]*lang="zh-CN"/);
	assert.ok(chinese.includes("Go 查询，简单一点。"));
	const files = await htmlFiles(build);
	assert.equal(
		files.filter((file) => file.endsWith("index.html")).length,
		18,
		"Each language must publish one homepage and eight guides without fallback duplicates",
	);
	for (const file of files) {
		const html = await readFile(file, "utf8");
		if (!html.includes("<article")) continue;
		assert.match(html, /<h1\b/);
		assert.ok(!html.includes("React Query"), "Documentation contains an unrelated comparison");
		for (const match of html.matchAll(/(?:href|src)="([^"]+)"/g)) {
			await assertDestination(match[1].replaceAll("&amp;", "&"), file);
		}
	}
});

test("content pages publish distinct localized metadata and a usable sharing image", async () => {
	const descriptions = new Set<string>();
	for (const locale of ["en", "zh-CN"]) {
		const prefix = locale === "en" ? "" : "zh-CN/";
		for (const route of [
			"",
			"docs/",
			"docs/queries/",
			"docs/query-options/",
			"docs/cache/",
			"docs/invalidation/",
			"docs/observability/",
			"docs/mcp/",
			"docs/versioning/",
		]) {
			const html = await readFile(path.join(build, prefix, route, "index.html"), "utf8");
			const title = html.match(/<title>([^<]+)<\/title>/)?.[1];
			assert.ok(
				title && title !== "cacheq | cacheq",
				`Missing descriptive title: ${prefix}${route}`,
			);
			const metadata = [...html.matchAll(/<meta\b[^>]*>/g)].map((match) => match[0]);
			const descriptionTags = metadata.filter((tag) => tag.includes('name="description"'));
			assert.equal(descriptionTags.length, 1);
			const description = descriptionTags[0].match(/content="([^"]+)"/)?.[1];
			assert.ok(description, `Missing description: ${prefix}${route}`);
			assert.ok(!descriptions.has(description), `Repeated description: ${prefix}${route}`);
			descriptions.add(description);
			assert.match(description, /cacheq/);
			assert.equal(/[\u4e00-\u9fff]/u.test(description), locale === "zh-CN");
			for (const attribute of ['property="og:image"', 'name="twitter:image"']) {
				const images = metadata.filter((tag) => tag.includes(attribute));
				assert.equal(images.length, 1);
				assert.ok(images[0].includes('content="https://waterkyuu.github.io/cacheq/cacheq.png"'));
			}
			const cards = metadata.filter((tag) => tag.includes('name="twitter:card"'));
			assert.equal(cards.length, 1);
			assert.ok(cards[0].includes('content="summary"'));
			if (!route) {
				assert.match(title, /Go/);
				const types = metadata.filter((tag) => tag.includes('property="og:type"'));
				assert.equal(types.length, 1);
				assert.ok(types[0].includes('content="website"'));
			}
		}
	}
	assert.equal(descriptions.size, 18);
	assert.ok((await readFile(path.join(build, "cacheq.png"))).length > 0);
});

test("Go search results resolve to published sections and public source declarations", async () => {
	const index: SearchRecord[] = JSON.parse(
		await readFile(path.join(build, "search-index.json"), "utf8"),
	);
	const documents = index.filter((record) => record.url.startsWith("/cacheq/"));
	assert.ok(
		documents.some((record) => record.language === "en" && record.url.startsWith("/cacheq/docs/")),
	);
	assert.ok(
		documents.some(
			(record) =>
				record.language === "zh-CN" &&
				record.url.startsWith("/cacheq/zh-CN/docs/") &&
				record.url.includes("#"),
		),
	);
	assert.ok(index.some((record) => record.title === "Client.Invalidate"));
	assert.ok(index.some((record) => record.title === "Client.Stats"));
	assert.ok(index.some((record) => record.title === "Query"));
	for (const record of documents)
		await assertDestination(record.url, path.join(build, "index.html"));
});
