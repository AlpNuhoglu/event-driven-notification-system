window.onload = function () {
  window.ui = SwaggerUIBundle({
    url: "/openapi.yaml",
    dom_id: "#swagger-ui",
    deepLinking: true,
    validatorUrl: null,
    presets: [SwaggerUIBundle.presets.apis],
    layout: "BaseLayout"
  });
};
