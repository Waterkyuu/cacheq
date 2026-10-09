import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import react from "@astrojs/react";

export default defineConfig({
	site: "https://waterkyuu.github.io",
	base: "/cacheq",
	trailingSlash: "always",
	outDir: "./build",
	publicDir: "../assets",
	integrations: [
		react(),
		starlight({
			title: "cacheq",
			description: "One client, many types. Shared cache and background refresh for Go.",
			logo: { src: "../assets/cacheq.png", alt: "cacheq" },
			favicon: "/cacheq.png",
			head: [
				{
					tag: "meta",
					attrs: { property: "og:image", content: "https://waterkyuu.github.io/cacheq/cacheq.png" },
				},
				{ tag: "meta", attrs: { property: "og:image:alt", content: "cacheq logo" } },
				{
					tag: "meta",
					attrs: {
						name: "twitter:image",
						content: "https://waterkyuu.github.io/cacheq/cacheq.png",
					},
				},
				{ tag: "meta", attrs: { name: "twitter:image:alt", content: "cacheq logo" } },
				// The existing square logo fits a summary card without a landscape crop.
				{ tag: "meta", attrs: { name: "twitter:card", content: "summary" } },
			],
			locales: {
				root: { label: "English", lang: "en" },
				"zh-CN": { label: "简体中文", lang: "zh-CN" },
			},
			sidebar: [
				{ slug: "docs", label: "Getting started", translations: { "zh-CN": "开始使用" } },
				{
					slug: "docs/queries",
					label: "Queries & conditions",
					translations: { "zh-CN": "查询与条件请求" },
				},
				{
					slug: "docs/query-options",
					label: "Query options",
					translations: { "zh-CN": "查询配置" },
				},
				{
					slug: "docs/cache",
					label: "Cache & lifecycle",
					translations: { "zh-CN": "缓存与生命周期" },
				},
				{
					slug: "docs/batching",
					label: "Automatic batch loading",
					translations: { "zh-CN": "自动批量加载" },
				},
				{ slug: "docs/invalidation", label: "Invalidation", translations: { "zh-CN": "缓存失效" } },
				{
					slug: "docs/observability",
					label: "Observability",
					translations: { "zh-CN": "可观测性" },
				},
				{
					slug: "docs/mcp",
					label: "MCP resource caching",
					translations: { "zh-CN": "MCP 资源缓存" },
				},
				{
					slug: "docs/events",
					label: "Diagnostic events",
					translations: { "zh-CN": "诊断事件" },
				},
				{
					slug: "docs/bubbletea",
					label: "Bubble Tea example",
					translations: { "zh-CN": "Bubble Tea 示例" },
				},
			],
			social: [{ icon: "github", label: "GitHub", href: "https://github.com/Waterkyuu/cacheq" }],
			customCss: ["./src/css/custom.css"],
			pagefind: false,
			expressiveCode: { themes: ["github-light", "github-dark"] },
			components: {
				Search: "./src/components/search.astro",
				MarkdownContent: "./src/components/markdown-content.astro",
				PageTitle: "./src/components/page-title.astro",
			},
		}),
	],
});
