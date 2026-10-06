import React, { useCallback, useEffect, useRef, useState } from "react";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";

interface SearchRecord {
	title: string;
	url: string;
	language: string;
	content: string;
}

export default function SearchBar() {
	const { i18n, siteConfig } = useDocusaurusContext();
	const chinese = i18n.currentLocale === "zh-CN";
	const indexURL = siteConfig.customFields?.searchIndexPath as string;
	const dialog = useRef<HTMLDialogElement>(null);
	const input = useRef<HTMLInputElement>(null);
	const index = useRef<SearchRecord[] | null>(null);
	const [query, setQuery] = useState("");
	const [records, setRecords] = useState<SearchRecord[]>([]);
	const [status, setStatus] = useState<"idle" | "loading" | "ready" | "error">("idle");

	const open = useCallback(async () => {
		if (!dialog.current?.open) dialog.current?.showModal();
		input.current?.focus();
		if (index.current) return;
		setStatus("loading");
		try {
			const response = await fetch(indexURL);
			if (!response.ok) throw new Error("Search index unavailable");
			const data: SearchRecord[] = await response.json();
			index.current = data;
			setRecords(data);
			setStatus("ready");
		} catch {
			setStatus("error");
		}
	}, [indexURL]);
	useEffect(() => {
		const handleKey = (event: KeyboardEvent) => {
			if (
				event.key === "/" &&
				!(
					event.target instanceof Element &&
					event.target.closest('input, textarea, [contenteditable="true"]')
				)
			) {
				event.preventDefault();
				void open();
			}
		};
		document.addEventListener("keydown", handleKey);
		return () => document.removeEventListener("keydown", handleKey);
	}, [open]);

	const words = query.trim().toLowerCase().split(/\s+/).filter(Boolean);
	const matches = words.length
		? records
				.filter((record) => record.language === i18n.currentLocale)
				.filter((record) =>
					words.every((word) => (record.title + " " + record.content).toLowerCase().includes(word)),
				)
				.map((record) => ({
					...record,
					score: words.reduce(
						(score, word) =>
							score +
							(record.title.toLowerCase().includes(word)
								? 5
								: record.content.toLowerCase().includes(word)
									? 1
									: -100),
						0,
					),
				}))
				.filter((record) => record.score > 0)
				.sort((a, b) => b.score - a.score || a.title.localeCompare(b.title))
				.slice(0, 12)
		: [];
	const snippet = (content: string) => {
		const position = content.toLowerCase().indexOf(words[0] ?? "");
		return content.slice(Math.max(0, position - 40), Math.max(0, position - 40) + 160);
	};
	return (
		<>
			<button
				type="button"
				className="local-search-button"
				onClick={() => void open()}
				aria-haspopup="dialog"
				aria-label={chinese ? "搜索文档" : "Search documentation"}
			>
				<svg
					width="16"
					height="16"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					strokeWidth="1.7"
					aria-hidden="true"
				>
					<circle cx="10" cy="10" r="6.5" />
					<path d="m15 15 5 5" />
				</svg>
				<span>{chinese ? "搜索文档" : "Search docs"}</span>
				<kbd>/</kbd>
			</button>
			<dialog
				ref={dialog}
				className="local-search-dialog"
				aria-label={chinese ? "搜索文档" : "Search documentation"}
				onClick={(event) => {
					if (event.target === dialog.current) dialog.current?.close();
				}}
			>
				<button
					type="button"
					className="local-search-close"
					onClick={() => dialog.current?.close()}
					aria-label={chinese ? "关闭搜索" : "Close search"}
				>
					Esc
				</button>
				<label htmlFor="docs-search">{chinese ? "搜索文档" : "Search documentation"}</label>
				<input
					ref={input}
					id="docs-search"
					className="local-search-input"
					type="search"
					value={query}
					onChange={(event) => setQuery(event.target.value)}
					autoComplete="off"
					placeholder={chinese ? "查询、缓存、Invalidate…" : "Queries, cache, Invalidate…"}
					onKeyDown={(event) => {
						if (event.key === "Enter" && matches[0]) window.location.assign(matches[0].url);
					}}
				/>
				<div className="local-search-results" aria-live="polite">
					{status === "loading" && <p>{chinese ? "正在加载索引…" : "Loading index…"}</p>}
					{status === "error" && (
						<p>
							{chinese
								? "搜索暂时不可用，请使用侧边导航。"
								: "Search unavailable. Use the chapter navigation."}
						</p>
					)}
					{matches.map((record) => (
						<a key={`${record.language}:${record.url}:${record.title}`} href={record.url}>
							<strong>{record.title}</strong>
							<p>{snippet(record.content)}</p>
						</a>
					))}
					{status === "ready" && words.length > 0 && !matches.length && (
						<p>{chinese ? "没有找到结果，试试 API 名称。" : "No matches. Try an API name."}</p>
					)}
				</div>
				<p className="local-search-hint">
					{chinese
						? "输入功能名称或 API · Enter 打开首项 · Esc 关闭"
						: "Search a feature or API · Enter opens first result · Esc closes"}
				</p>
			</dialog>
		</>
	);
}
