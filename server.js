// server.js
const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 3001;

// Middlewares
app.use(cors());
app.use(express.json({ limit: "100mb" }));
app.use(express.urlencoded({ limit: "100mb", extended: true }));

// Criar pool de conexões para PostgreSQL
// A variável DATABASE_URL é a URL INTERNA COMPLETA
const isProduction = process.env.NODE_ENV === "production";

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Adicione estas linhas para forçar o SSL de forma explícita e correta
  ssl: isProduction ? { rejectUnauthorized: false } : false,
});


// Rota teste
app.get("/", (req, res) => {
  res.send("Servidor Node + PostgreSQL rodando! 🚀");
});

// Criar tabela para histórico de ações (se não existir)
const createTableQuery = `
CREATE TABLE IF NOT EXISTS stocks_history (
  id SERIAL PRIMARY KEY,
  ticker TEXT NOT NULL,
  date BIGINT NOT NULL,
  preco_abertura REAL,
  preco_fechamento REAL,
  preco_maximo REAL,
  preco_minimo REAL,
  preco_medio REAL,
  quantidade_negociada BIGINT,
  quantidade_negocios BIGINT,
  volume_negociado REAL,
  fator_ajuste REAL,
  preco_fechamento_ajustado REAL,
  fator_ajuste_desdobramentos REAL,
  preco_fechamento_ajustado_desdobramentos REAL,
  UNIQUE(ticker, date)
);
`;

pool.query(createTableQuery)
  .then(() => console.log("Tabela stocks_history pronta!"))
  .catch((err) => console.error("Erro criando tabela:", err));

// Inserir histórico completo (POST)
app.post("/stocks_history", async (req, res) => {
  const jsonData = req.body;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    for (const item of jsonData) {
      const timestamp = item.date
        ? Number(item.date)
        : item.data
        ? new Date(item.data).getTime()
        : Date.now();

      await client.query(
        `INSERT INTO stocks_history (
          ticker, date, preco_abertura, preco_fechamento, preco_maximo,
          preco_minimo, preco_medio, quantidade_negociada, quantidade_negocios,
          volume_negociado, fator_ajuste, preco_fechamento_ajustado,
          fator_ajuste_desdobramentos, preco_fechamento_ajustado_desdobramentos
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (ticker, date) DO UPDATE SET
          preco_abertura = EXCLUDED.preco_abertura,
          preco_fechamento = EXCLUDED.preco_fechamento,
          preco_maximo = EXCLUDED.preco_maximo,
          preco_minimo = EXCLUDED.preco_minimo,
          preco_medio = EXCLUDED.preco_medio,
          quantidade_negociada = EXCLUDED.quantidade_negociada,
          quantidade_negocios = EXCLUDED.quantidade_negocios,
          volume_negociado = EXCLUDED.volume_negociado,
          fator_ajuste = EXCLUDED.fator_ajuste,
          preco_fechamento_ajustado = EXCLUDED.preco_fechamento_ajustado,
          fator_ajuste_desdobramentos = EXCLUDED.fator_ajuste_desdobramentos,
          preco_fechamento_ajustado_desdobramentos = EXCLUDED.preco_fechamento_ajustado_desdobramentos
        `,
        [
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
          item.preco_fechamento_ajustado_desdobramentos,
        ]
      );
    }

    await client.query("COMMIT");
    res.json({ message: "Dados inseridos com sucesso!" });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error(err);
    res.status(500).json({ error: err.message });
  } finally {
    client.release();
  }
});

// Buscar histórico de um ticker (GET)
app.get("/stocks_history/:ticker", async (req, res) => {
  const ticker = req.params.ticker.toUpperCase();

  try {
    const result = await pool.query(
      "SELECT * FROM stocks_history WHERE ticker = $1 ORDER BY date ASC",
      [ticker]
    );

    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Deletar histórico de um ticker (DELETE)
app.delete("/stocks_history/:ticker", async (req, res) => {
  const ticker = req.params.ticker.toUpperCase();

  try {
    const result = await pool.query(
      "DELETE FROM stocks_history WHERE ticker = $1",
      [ticker]
    );
    res.json({ message: `Histórico de ${ticker} deletado!`, changes: result.rowCount });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Deletar todos os históricos (DELETE)
app.delete("/stocks_history", async (req, res) => {
  try {
    const result = await pool.query("DELETE FROM stocks_history");
    res.json({ message: "Todos os históricos deletados!", changes: result.rowCount });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Buscar todos os tickers únicos
app.get("/tickers", async (req, res) => {
  try {
    const result = await pool.query("SELECT DISTINCT ticker FROM stocks_history");
    const tickers = result.rows.map(r => r.ticker);
    res.json(tickers);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message });
  }
});

// Iniciar servidor
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Servidor rodando em http://localhost:${PORT}`);
});
