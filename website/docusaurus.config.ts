import type { Config } from "@docusaurus/types";
import path from "node:path";
import { themes } from "prism-react-renderer";
const baseUrl = "/cacheq/";
const sourceBranch = process.env.DOCS_REF || "waterkyuu/feat/docs-site";

const config: Config = {
	title: "cacheq",
	tagline: "Typed queries. Shared data. Clearer Go.",
	url: "https://waterkyuu.github.io",
	baseUrl,
	customFields: { searchIndexPath: `${baseUrl}search-index.json` },
	trailingSlash: true,
	favicon: "cacheq.png",
	staticDirectories: ["../assets"],
	organizationName: "Waterkyuu",
	projectName: "cacheq",
	onBrokenLinks: "throw",
	markdown: { format: "md", hooks: { onBrokenMarkdownLinks: "throw" } },
	i18n: {
		defaultLocale: "en",
		locales: ["en", "zh-CN"],
		localeConfigs: {
			"zh-CN": { label: "简体中文", htmlLang: "zh-CN" },
			en: { label: "English", htmlLang: "en" },
		},
	},
	plugins: [
		function localSearchTheme() {
			return {
				name: "cacheq-local-search",
				configureWebpack() {
					return {
						resolve: {
							alias: {
								"@theme/SearchBar": path.resolve(__dirname, "src/theme/search-bar/index.tsx"),
							},
						},
					};
				},
			};
		},
	],
	presets: [
		[
			"classic",
			{
				docs: {
					path: "generated-docs",
					routeBasePath: "/docs",
					sidebarPath: "./sidebars.ts",
					editUrl: ({ locale, docPath }: { locale: string; docPath: string }) => {
						const name = docPath.replace(/\.md$/, "");
						const suffix = locale === "en" ? "en-US" : "zh-CN";
						return `https://github.com/Waterkyuu/cacheq/edit/${sourceBranch}/docs/${name}.${suffix}.md`;
					},
				},
				blog: false,
				pages: {},
				theme: { customCss: "./src/css/custom.css" },
			},
		],
	],
	themeConfig: {
		colorMode: { defaultMode: "light", disableSwitch: false, respectPrefersColorScheme: true },
		navbar: {
			title: "cacheq",
			logo: { alt: "cacheq", src: "cacheq.png" },
			items: [
				{ type: "docSidebar", sidebarId: "guides", position: "left", label: "Docs" },
				{ type: "search", position: "right" },
				{ type: "localeDropdown", position: "right" },
				{ href: "https://github.com/Waterkyuu/cacheq", label: "GitHub", position: "right" },
			],
		},
		footer: {
			style: "light",
			links: [
				{
					title: "cacheq",
					items: [
						{ label: "GitHub", href: "https://github.com/Waterkyuu/cacheq" },
						{ label: "Go reference", href: "https://pkg.go.dev/github.com/Waterkyuu/cacheq" },
					],
				},
			],
			copyright: "cacheq · Go 1.22+ · MIT",
		},
		prism: {
			theme: themes.github,
			darkTheme: themes.dracula,
			additionalLanguages: ["go", "bash", "json", "yaml"],
		},
	},
};

export default config;
