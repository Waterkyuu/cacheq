import { defineCollection } from "astro:content";
import { docsLoader } from "@astrojs/starlight/loaders";
import { docsSchema } from "@astrojs/starlight/schema";

export const collections = {
	docs: defineCollection({
		// Preserve the published zh-CN path instead of lowercasing its locale directory.
		loader: docsLoader({
			generateId: ({ entry }) => entry.replace(/\.(md|mdx)$/, "").replace(/\/index$/, ""),
		}),
		schema: docsSchema(),
	}),
};
