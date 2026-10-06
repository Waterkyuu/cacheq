import React from "react";
import Layout from "@theme/Layout";
import Link from "@docusaurus/Link";
import CodeBlock from "@theme/CodeBlock";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import useBaseUrl from "@docusaurus/useBaseUrl";

const queryCode = `client := cacheq.NewClient(cacheq.Options{
    StaleTime: time.Minute,
})
defer client.Close()

profile := cacheq.Query(client, "user:42", loadUser)
users := cacheq.Query(client, "users", loadUsers)
defer profile.Close()
defer users.Close()`;

export default function Home() {
	const { i18n } = useDocusaurusContext();
	const chinese = i18n.currentLocale === "zh-CN";
	const docs = useBaseUrl("/docs/");
	const logo = useBaseUrl("/cacheq.png");
	const guides = [
		{
			slug: "queries",
			title: chinese ? "查询与条件请求" : "Queries & conditions",
			description: chinese
				? "共享请求，读取状态，满足条件再加载。"
				: "Share requests, read state, and load when ready.",
			api: "Query · SetEnabled · Updates",
		},
		{
			slug: "cache",
			title: chinese ? "缓存与生命周期" : "Cache & lifecycle",
			description: chinese
				? "复用新鲜数据，后台刷新，自动清理缓存。"
				: "Reuse data, refresh in the background, and clean up.",
			api: "StaleTime · GCTime · Prefetch",
		},
		{
			slug: "invalidation",
			title: chinese ? "缓存失效" : "Invalidation",
			description: chinese
				? "把相关缓存一起作废，选择何时重新请求。"
				: "Invalidate related data and choose when to refetch.",
			api: "InvalidateMany · InvalidateWhere",
		},
	];
	return (
		<Layout
			title={chinese ? "Go 的类型安全查询客户端" : "Typed queries for Go"}
			description={
				chinese
					? "一个客户端，多种类型。共享缓存，后台刷新，按需失效。"
					: "One client, many types. Shared cache, background refresh, and focused invalidation."
			}
		>
			<main className="home">
				<article>
					<section className="home-intro">
						<img className="home-logo" src={logo} width="88" height="88" alt="cacheq" />
						<h1>{chinese ? "Go 查询，简单一点。" : "Go queries, made simple."}</h1>
						<p>
							{chinese ? (
								<>
									一个客户端，多种数据类型。
									<br />
									共享缓存、后台刷新与按需失效。
								</>
							) : (
								<>
									One client. Many data types.
									<br />
									Shared cache, background refresh, and focused invalidation.
								</>
							)}
						</p>
						<Link className="home-button" to={docs}>
							{chinese ? "开始使用" : "Get started"}
						</Link>
						<div className="install-snippet">
							<CodeBlock language="bash">go get github.com/Waterkyuu/cacheq</CodeBlock>
						</div>
						<span className="home-requirements">
							Go 1.22+ <span>·</span> {chinese ? "核心库零依赖" : "Zero core dependencies"}{" "}
							<span>·</span> MIT
						</span>
					</section>
					<section className="home-guides" aria-label={chinese ? "功能指南" : "Feature guides"}>
						{guides.map((guide) => (
							<Link key={guide.slug} className="guide-card" to={docs + guide.slug + "/"}>
								<h2>
									{guide.title}
									<span aria-hidden="true">↗</span>
								</h2>
								<p>{guide.description}</p>
								<code>{guide.api}</code>
							</Link>
						))}
					</section>
					<section className="home-code">
						<h2>{chinese ? "一个客户端，多种类型。" : "One client. Different result types."}</h2>
						<p>
							{chinese
								? "用户详情与用户列表共用缓存，各自保留类型。"
								: "Profiles and lists share the same client, while keeping their types."}
						</p>
						<CodeBlock language="go">{queryCode}</CodeBlock>
						<Link className="home-inline-link" to={docs + "queries/"}>
							{chinese ? "查看完整用法" : "Read the complete guide"}{" "}
							<span aria-hidden="true">→</span>
						</Link>
					</section>
				</article>
			</main>
		</Layout>
	);
}
