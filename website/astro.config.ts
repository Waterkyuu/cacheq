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
					slug: "docs/cache",
					label: "Cache & lifecycle",
					translations: { "zh-CN": "缓存与生命周期" },
				},
				{ slug: "docs/invalidation", label: "Invalidation", translations: { "zh-CN": "缓存失效" } },
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
