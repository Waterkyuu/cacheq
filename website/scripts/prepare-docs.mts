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
		zh: "失效与刷新",
		en: "Invalidation & refresh",
		description: {
			en: "Invalidate cacheq queries by key, batch, or predicate. Await post-invalidation refreshes and coalesce bounded follow-up loads after mutations.",
			"zh-CN":
				"在 Go 中使用 cacheq 按键、批量或条件使缓存失效，等待关联刷新，并在加载期间失效后执行有界补刷。",
		},
	},
	{
		id: "shared-cache",
		zh: "共享结果缓存",
		en: "Shared result cache",
		description: {
			en: "Configure optional Redis result caching for typed cacheq queries. Preserve source freshness, invalidate shared keys, fence obsolete writes, and handle cache failures.",
			"zh-CN":
				"为 cacheq 类型化查询配置可选 Redis 结果缓存，保留数据源新鲜期，按 key 共享失效，拒绝旧值回填并处理缓存故障。",
		},
	},
	{
		id: "query-filters",
		zh: "统一查询筛选",
		en: "Query filters",
		description: {
			en: "Reuse cacheq QueryFilter selections across invalidation, awaited refresh, cancellation, and removal in Go. Combine exact keys and predicates with tenant-scoped query keys.",
			"zh-CN":
				"在 Go 中通过 cacheq QueryFilter 为失效、等待刷新、取消和删除复用同一组查询筛选，结合精确键、条件函数和包含租户范围的查询键。",
		},
	},
	{
		id: "shared-invalidation",
		zh: "共享失效通知与标签",
		en: "Shared invalidation & tags",
		description: {
			en: "Receive cacheq Redis invalidation notifications, register remote query tags, recover from connection gaps, and manage watcher ownership and conditional generation checks.",
			"zh-CN":
				"接收 cacheq Redis 失效通知，登记远端查询标签，修复连接中断区间，管理 watcher 生命周期并执行原子版本校验。",
		},
	},
	{
		id: "query-tags",
		zh: "查询标签",
		en: "Query tags",
		description: {
			en: "Group related cacheq queries with Definition.Tags and select labels through QueryFilter for invalidation, awaited refresh, cancellation, and removal in Go.",
			"zh-CN":
				"在 Go 中通过 cacheq Definition.Tags 声明关联查询分组，使用 QueryFilter 按标签失效、等待刷新、取消和删除，并管理标签登记与生命周期。",
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
			en: "Create reusable typed queries with cacheq QueryDefinition and shared QueryOptions across reading, subscribing, and prefetching in Go. Configure freshness, retries, timeouts, and automatic loading.",
			"zh-CN":
				"在 Go 中通过 cacheq QueryDefinition 创建可复用的类型化查询，并在读取、订阅和预加载间共享 QueryOptions，配置新鲜期、重试、超时和自动加载。",
		},
	},
	{
		id: "batching",
		zh: "自动批量加载",
		en: "Automatic batch loading",
		description: {
			en: "Group concurrent cacheq loads or load known keys with LoadMany in Go. Configure bounded bulk callbacks, handle ordered per-key outcomes, and integrate batching with cached queries.",
			"zh-CN":
				"使用 cacheq 在 Go 中合并并发加载，或通过 LoadMany 加载已知 key。限制批次大小，按输入顺序处理逐 key 结果，并与查询缓存及取消策略集成。",
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
	{
		id: "updates",
		zh: "原子缓存更新",
		en: "Atomic cache updates",
		description: {
			en: "Transform cached values with revision checks and explicit conflicts in cacheq.",
			"zh-CN": "使用 cacheq 在锁外转换已有缓存值，通过版本检查识别并发修改，防止覆盖新数据。",
		},
	},
	{
		id: "mutations",
		zh: "写入与关联失效",
		en: "Mutations & synchronization",
		description: {
			en: "Execute typed cacheq mutations, observe write state, and synchronize associated queries after a confirmed commit.",
			"zh-CN":
				"使用 cacheq 执行类型安全写入、订阅执行状态，并在提交成功后更新缓存、使关联查询失效。",
		},
	},
	{
		id: "execution",
		zh: "执行并发预算",
		en: "Execution budgets",
		description: {
			en: "Bound cacheq loader, mutation, and batch concurrency with reusable execution budgets, FIFO queues, and cancellation.",
			"zh-CN": "通过 cacheq 可共享的执行预算限制查询、写入和批次并发，管理 FIFO 排队、过载与取消。",
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
