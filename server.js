// server.js
const express = require("express");
const cors = require("cors");
const sqlite3 = require("sqlite3").verbose();

const app = express();
const PORT = process.env.PORT || 3001;

// Middlewares
app.use(cors());
app.use(express.json({ limit: "100mb" }));
app.use(express.urlencoded({ limit: "100mb", extended: true }));

// Conectar ao banco SQLite (cria se não existir)
const db = new sqlite3.Database("./database.sqlite", (err) => {
  if (err) {
    console.error("Erro ao abrir o banco:", err.message);
  } else {
    console.log("Banco conectado com sucesso!");
  }
});

// Criar tabela para histórico de ações
db.run(`
  CREATE TABLE IF NOT EXISTS stocks_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    date INTEGER NOT NULL,
    preco_abertura REAL,
    preco_fechamento REAL,
    preco_maximo REAL,
    preco_minimo REAL,
    preco_medio REAL,
    quantidade_negociada INTEGER,
    quantidade_negocios INTEGER,
    volume_negociado REAL,
    fator_ajuste REAL,
    preco_fechamento_ajustado REAL,
    fator_ajuste_desdobramentos REAL,
    preco_fechamento_ajustado_desdobramentos REAL,
    UNIQUE(ticker, date)
  )
`);

// Rota teste
app.get("/", (req, res) => {
  res.send("Servidor Node + SQLite rodando! 🚀");
});

// Inserir histórico completo (POST)
app.post("/stocks_history", (req, res) => {
  const jsonData = req.body; // Recebe array de objetos

  const stmt = db.prepare(`
    INSERT OR REPLACE INTO stocks_history (
      ticker, date, preco_abertura, preco_fechamento, preco_maximo,
      preco_minimo, preco_medio, quantidade_negociada, quantidade_negocios,
      volume_negociado, fator_ajuste, preco_fechamento_ajustado,
      fator_ajuste_desdobramentos, preco_fechamento_ajustado_desdobramentos
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  jsonData.forEach((item) => {
    let timestamp;

    if (item.date) {
      // Se veio timestamp ou string numérica
      timestamp = Number(item.date);
    } else if (item.data) {
      // fallback para "data" se existir
      if (typeof item.data === "string") {
        const [day, month, year] = item.data.split("/");
        timestamp = new Date(Number(year), Number(month) - 1, Number(day)).getTime();
      } else if (typeof item.data === "number") {
        timestamp = item.data;
      } else {
        timestamp = Date.now();
      }
    } else {
      timestamp = Date.now();
    }

    stmt.run(
      item.ticker,
      timestamp,
      item.preco_abertura,
      item.preco_fechamento,
      item.preco_maximo,
      item.preco_minimo,
      item.preco_medio,
      item.quantidade_negociada,
      item.quantidade_negocios,
      item.volume_negociado,
      item.fator_ajuste,
      item.preco_fechamento_ajustado,
      item.fator_ajuste_desdobramentos,
      item.preco_fechamento_ajustado_desdobramentos
    );
  });

  stmt.finalize();
  res.json({ message: "Dados inseridos com sucesso!" });
});

// Buscar histórico de um ticker (GET)
app.get("/stocks_history/:ticker", (req, res) => {
  const ticker = req.params.ticker.toUpperCase();
  db.all(
    "SELECT * FROM stocks_history WHERE ticker = ? ORDER BY date ASC",
    [ticker],
    (err, rows) => {
      if (err) return res.status(500).json({ error: err.message });

      // Garantir que date seja número
      const formattedRows = rows.map((r) => ({
        ...r,
        date: Number(r.date),
      }));

      res.json(formattedRows);
    }
  );
});

// Deletar histórico de um ticker (DELETE)
app.delete("/stocks_history/:ticker", (req, res) => {
  const ticker = req.params.ticker.toUpperCase();
  db.run(
    "DELETE FROM stocks_history WHERE ticker = ?",
    [ticker],
    function (err) {
      if (err) return res.status(500).json({ error: err.message });
      res.json({ message: `Histórico de ${ticker} deletado!`, changes: this.changes });
    }
  );
});

// Deletar todos os históricos (DELETE)
app.delete("/stocks_history", (req, res) => {
  db.run("DELETE FROM stocks_history", function (err) {
    if (err) return res.status(500).json({ error: err.message });
    res.json({ message: "Todos os históricos deletados!", changes: this.changes });
  });
});

// Iniciar servidor
app.listen(process.env.PORT || 3001, '0.0.0.0', () => {
  console.log(`Servidor rodando em http://localhost:${process.env.PORT || 3001}`);
});