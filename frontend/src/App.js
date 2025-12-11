import { BrowserRouter as Router, Routes, Route } from "react-router";
import "./App.css";
import HomeComponent from "./HomeComponent";
import TickerPage from "./TickerPage";

function App() {
  return (
    <Router>
      <div className="App">
        <Routes>
          <Route
            path="/"
            element={<HomeComponent dataUrl="http://127.0.0.1:8000/sp100" />}
          />

          <Route path="/:ticker" element={<TickerPage />} />
        </Routes>
      </div>
    </Router>
  );
}

export default App;
