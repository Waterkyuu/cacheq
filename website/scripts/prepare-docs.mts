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
	const suffix = locale === "en" ? "en-US" : "zh-CN";
	const title = source.match(/^# (.+)$/m)?.[1] ?? label;
	const header = [
		"---",
		`title: ${JSON.stringify(title)}`,
		`sidebar: { label: ${JSON.stringify(label)}, order: ${position} }`,
		`editUrl: https://github.com/Waterkyuu/cacheq/edit/${branch}/docs/${chapter.id}.${suffix}.md`,
		"---",
		"",
	];
	const body = source
		.replace(/^# .+\n*/, "")
		.replace(/^\[(?:English|简体中文)\].*$/gm, "")
		.replace(/\]\(([^)]+)\.(?:en-US|zh-CN)\.md(#[^)]*)?\)/g, (_, name, anchor) => {
			const prefix = locale === "en" ? "/cacheq/docs/" : "/cacheq/zh-CN/docs/";
			const route = name === "getting-started" ? "" : `${name}/`;
			return `](${prefix}${route}${anchor ?? ""})`;
		})
		.replace(
			/\]\(\.\.\/([^)]*\.go)\)/g,
			`](https://github.com/Waterkyuu/cacheq/blob/${branch}/$1)`,
		);
	return header.join("\n") + body;
}

// Regenerate only the ignored directories owned by this build step.
export async function prepareDocs(root: string) {
	const branch = process.env.DOCS_REF || "waterkyuu/feat/docs-site";
	const generated = path.join(root, "src/content/docs");
	await rm(generated, { recursive: true, force: true });
	for (const locale of ["en", "zh-CN"]) {
		const localeRoot = locale === "en" ? generated : path.join(generated, "zh-CN");
		const destination = path.join(localeRoot, "docs");
		await mkdir(destination, { recursive: true });
		for (const [position, chapter] of chapters.entries()) {
			const suffix = locale === "en" ? "en-US" : "zh-CN";
			const source = await readFile(
				path.join(root, "../docs", `${chapter.id}.${suffix}.md`),
				"utf8",
			);
			await writeFile(
				path.join(destination, chapter.id === "getting-started" ? "index.md" : `${chapter.id}.md`),
				prepareMarkdown(source, chapter, locale, position + 1, branch),
			);
		}
		const component =
			locale === "en" ? "../../components/home.astro" : "../../../components/home.astro";
		await writeFile(
			path.join(localeRoot, "index.mdx"),
			[
				"---",
				"title: cacheq",
				"template: splash",
				"editUrl: false",
				"---",
				"",
				`import Home from '${component}';`,
				"",
				`<Home chinese={${locale === "zh-CN"}} />`,
				"",
			].join("\n"),
		);
	}
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
	await prepareDocs(path.resolve(path.dirname(fileURLToPath(import.meta.url)), ".."));
}
