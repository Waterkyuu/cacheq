import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

export interface Chapter {
	id: string;
	zh: string;
	en: string;
}

export const chapters: Chapter[] = [
	{ id: "getting-started", zh: "开始使用", en: "Getting started" },
	{ id: "queries", zh: "查询与条件请求", en: "Queries & conditions" },
	{ id: "cache", zh: "缓存与生命周期", en: "Cache & lifecycle" },
	{ id: "invalidation", zh: "缓存失效", en: "Invalidation" },
];

// Adapt repository documentation without creating another maintained copy.
export function prepareMarkdown(
	source: string,
	chapter: Chapter,
	locale: string,
	position: number,
	branch: string,
) {
	const label = chapter[locale === "en" ? "en" : "zh"];
	const header = [
		"---",
		`id: ${chapter.id}`,
		`sidebar_label: ${JSON.stringify(label)}`,
		`sidebar_position: ${position}`,
		...(chapter.id === "getting-started" ? ["slug: /"] : []),
		"---",
		"",
	];
	const body = source
		.replace(/^\[(?:English|简体中文)\].*$/gm, "")
		.replace(/\]\(([^)]+)\.(?:en-US|zh-CN)\.md(#[^)]*)?\)/g, "]($1.md$2)")
		.replace(
			/\]\(\.\.\/([^)]*\.go)\)/g,
			`](https://github.com/Waterkyuu/cacheq/blob/${branch}/$1)`,
		);
	return header.join("\n") + body;
}

// Regenerate only the ignored directories owned by this build step.
export async function prepareDocs(root: string) {
	const branch = process.env.DOCS_REF || "waterkyuu/feat/docs-site";
	// Remove the generated translation directory used before English became the default.
	await rm(path.join(root, "i18n/en/docusaurus-plugin-content-docs/current"), {
		recursive: true,
		force: true,
	});
	for (const locale of ["en", "zh-CN"]) {
		const destination =
			locale === "en"
				? path.join(root, "generated-docs")
				: path.join(root, "i18n/zh-CN/docusaurus-plugin-content-docs/current");
		await rm(destination, { recursive: true, force: true });
		await mkdir(destination, { recursive: true });
		for (const [position, chapter] of chapters.entries()) {
			const suffix = locale === "en" ? "en-US" : "zh-CN";
			const source = await readFile(
				path.join(root, "../docs", `${chapter.id}.${suffix}.md`),
				"utf8",
			);
			await writeFile(
				path.join(destination, `${chapter.id}.md`),
				prepareMarkdown(source, chapter, locale, position + 1, branch),
			);
		}
	}
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	await prepareDocs(path.resolve(path.dirname(fileURLToPath(import.meta.url)), ".."));
}
