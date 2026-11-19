import { createRoot } from "react-dom/client";
import "../../shared/theme.css";
import App from "./App.jsx";

const files = ["demo.json", "series.json"].map((f) => fetch(import.meta.env.BASE_URL + f).then((r) => r.json()));
Promise.all(files).then(([demo, series]) => {
  createRoot(document.getElementById("root")).render(<App demo={demo} series={series} />);
});
