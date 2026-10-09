import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

export interface Chapter {
	id: string;
	zh: string;
	en: string;
	description: { en: string; "zh-CN": string };
}

export const chapters: Chapter[] = [
	{
		id: "getting-started",
		zh: "开始使用",
		en: "Getting started",
		description: {
			en: "Install cacheq for Go 1.22+ and create your first typed query. Learn how one client shares cached data, deduplicates requests, and refreshes stale results.",
			"zh-CN":
				"安装适用于 Go 1.22 及以上版本的 cacheq，创建第一个类型安全查询，了解如何通过一个客户端共享缓存、合并请求并在后台刷新过期数据。",
		},
	},
	{
		id: "queries",
		zh: "查询与条件请求",
		en: "Queries & conditions",
		description: {
			en: "Use typed queries in Go with cacheq. Share requests across consumers, observe snapshots and updates, control conditional loading, and fetch or refetch data.",
			"zh-CN":
				"使用 cacheq 在 Go 中创建类型安全查询，共享请求，通过 Snapshot 和 Updates 读取状态，并控制条件加载、同步获取与手动刷新。",
		},
	},
	{
		id: "cache",
		zh: "缓存与生命周期",
		en: "Cache & lifecycle",
		description: {
			en: "Manage the cacheq shared cache in Go: read and write typed data, prefetch results, configure freshness, LRU eviction and maximum age, and release resources.",
			"zh-CN":
				"管理 cacheq 的 Go 共享缓存：读写类型安全数据、预取结果，配置新鲜时间、自动清理、LRU 容量淘汰与最大数据年龄，并正确释放资源。",
		},
	},
	{
		id: "invalidation",
		zh: "缓存失效",
		en: "Invalidation",
		description: {
			en: "Invalidate cacheq data in Go by key, batch, or predicate. Keep cached results available and choose background refresh or deferred loading after a mutation.",
			"zh-CN":
				"在 Go 中使用 cacheq 按单键、批量或条件使缓存失效，保留已有数据，并在业务更新后选择后台刷新或延迟加载。",
		},
	},
	{
		id: "observability",
		zh: "可观测性",
		en: "Observability",
		description: {
			en: "Measure cacheq activity in Go with per-client statistics: cache hits, shared requests, load outcomes and duration, retries, and cache cleanup counts.",
			"zh-CN":
				"通过 cacheq 的客户端统计衡量 Go 缓存收益，查看命中、请求合并、加载结果与耗时、重试和缓存清理次数。",
		},
	},
	{
		id: "mcp",
		zh: "MCP 资源缓存",
		en: "MCP resource caching",
		description: {
			en: "Integrate cacheq with an MCP server in Go: share resource reads across connections, preserve TTL, isolate private data, and invalidate cached values after mutations.",
			"zh-CN":
				"在 Go MCP 服务端接入 cacheq，跨连接共享资源读取，保留原始 TTL，隔离私有数据，并在修改后使缓存失效、通知客户端重新读取。",
		},
	},
	{
		id: "events",
		zh: "诊断事件",
		en: "Diagnostic events",
		description: {
			en: "Subscribe to cacheq diagnostic events in Go. Filter by key, trace load causes and shared requests, inspect retries and removals, and monitor bounded event delivery.",
			"zh-CN":
				"订阅 cacheq 的 Go 诊断事件，按 key 过滤，记录加载来源与原因、请求合并、重试及缓存删除，并监控有界事件投递。",
		},
	},
	{
		id: "versioning",
		zh: "版本与升级规则",
		en: "Versioning & upgrades",
		description: {
			en: "Understand cacheq's version policy: public API compatibility, v0 and stable release rules, deprecations, supported Go versions, and upgrade guidance.",
			"zh-CN":
				"了解 cacheq 的版本与升级规则：公开 API 兼容性、v0 与稳定版本的发布规则、弃用策略、Go 版本支持和升级方式。",
		},
	},
	{
		id: "query-options",
		zh: "查询配置",
		en: "Query options",
		description: {
			en: "Override cacheq Client defaults per query or fetch in Go. Configure independent freshness, filtered retries, and timeouts while sharing same-key data and requests.",
			"zh-CN":
				"在 Go 中通过 cacheq 的每次查询或获取调用覆盖客户端默认配置，独立判断新鲜度，设置重试错误判断和超时，同时共享同键数据与请求。",
		},
	},
	{
		id: "batching",
		zh: "自动批量加载",
		en: "Automatic batch loading",
		description: {
			en: "Group concurrent cacheq loads for different keys into bulk callbacks in Go. Configure collection windows, handle per-key results, and integrate batching with cached queries.",
			"zh-CN":
				"使用 cacheq 在 Go 中合并不同 key 的并发加载，配置批量收集窗口，处理逐 key 结果，并与查询缓存、重试及取消策略集成。",
		},
	},
	{
		id: "bubbletea",
		zh: "Bubble Tea 示例",
		en: "Bubble Tea example",
		description: {
			en: "Run the cacheq Bubble Tea task board in Go. Share query data across views, connect updates to the UI loop, refresh after mutations, and retain data when loads fail.",
			"zh-CN":
				"运行 cacheq 的 Go Bubble Tea 任务看板示例，跨视图共享查询，将更新接入界面消息循环，并处理修改后刷新、加载失败和资源清理。",
		},
	},
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
		`description: ${JSON.stringify(chapter.description[locale === "en" ? "en" : "zh-CN"])}`,
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
		.replace(/\]\(\.\.\/([^)]*\.go)\)/g, `](https://github.com/Waterkyuu/cacheq/blob/${branch}/$1)`)
		.replace(
			/\]\(\.\.\/(examples\/[^)]*\/)\)/g,
			`](https://github.com/Waterkyuu/cacheq/tree/${branch}/$1)`,
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
				`title: ${JSON.stringify(locale === "en" ? "Typed Query Client and Shared Cache for Go" : "Go 类型安全查询客户端与共享缓存")}`,
				`description: ${JSON.stringify(
					locale === "en"
						? "cacheq is a typed query client for Go with a shared in-memory cache, request deduplication, background refresh, and flexible invalidation. Zero core dependencies."
						: "cacheq 是适用于 Go 的类型安全查询客户端，提供进程内共享缓存、请求合并、后台刷新与灵活的缓存失效机制，核心库零依赖。",
				)}`,
				"template: splash",
				"editUrl: false",
				"head:",
				"  - tag: meta",
				"    attrs: { property: 'og:type', content: website }",
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
