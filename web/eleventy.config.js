import pluginWebc from "@11ty/eleventy-plugin-webc";

export default function (eleventyConfig) {
  eleventyConfig.addPlugin(pluginWebc, {
    components: "src/_components/**/*.webc",
  });

  eleventyConfig.addPassthroughCopy("src/**/*.js");
  eleventyConfig.addPassthroughCopy("src/**/*.css");
  eleventyConfig.addPassthroughCopy("src/fonts");
  eleventyConfig.addPassthroughCopy("src/images");

  eleventyConfig.setServerOptions({
    domDiff: false,
    hostname: "127.0.0.1",
    middleware: [
      function (req, _res, next) {
        // Rewrite /profile/<handle> to /profile/index.html
        if (req.url.match(/^\/profile\/[^/]+\/?$/)) {
          req.url = "/profile/index.html";
        }
        next();
      },
    ],
  });

  return {
    dir: {
      input: "src",
      output: "_site",
    },
  };
}
