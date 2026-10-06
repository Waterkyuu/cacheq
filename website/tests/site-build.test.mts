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
	const files = await htmlFiles(build);
	assert.ok(files.length >= 8, "Both languages must be built");
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

test("Go search results resolve to published sections and public source declarations", async () => {
	const index: SearchRecord[] = JSON.parse(
		await readFile(path.join(build, "search-index.json"), "utf8"),
	);
	const documents = index.filter((record) => record.url.startsWith("/cacheq/"));
	assert.ok(
		documents.some((record) => record.language === "en" && record.url.startsWith("/cacheq/en/")),
	);
	assert.ok(documents.some((record) => record.language === "zh-CN" && record.url.includes("#")));
	assert.ok(index.some((record) => record.title === "Client.InvalidateWhere"));
	assert.ok(index.some((record) => record.title === "Query"));
	for (const record of documents)
		await assertDestination(record.url, path.join(build, "index.html"));
});
