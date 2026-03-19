package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Evento struct {
	ID                   int    `json:"id"`
	Nome                 string `json:"nome"`
	IngressosDisponiveis int    `json:"ingressos_disponiveis"`
}

type ReservaRequest struct {
	EventoID  int `json:"evento_id"`
	UsuarioID int `json:"usuario_id"`
}

var db *pgxpool.Pool

func main() {
	dsn := buildDSN()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var err error
	db, err = pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("erro ao criar pool: %v", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		log.Fatalf("erro ao conectar no banco: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/eventos", handleListarEventos)
	mux.HandleFunc("/reservas", handleReservar)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}

	log.Println("API rodando na porta 8080")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("erro no servidor: %v", err)
	}
}

func buildDSN() string {
	host := getEnv("DB_HOST", "localhost")
	port := getEnv("DB_PORT", "5432")
	user := getEnv("DB_USER", "admin")
	pass := getEnv("DB_PASS", "123")
	name := getEnv("DB_NAME", "rinha")

	return "postgres://" + user + ":" + pass + "@" + host + ":" + port + "/" + name + "?pool_max_conns=10"
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func handleListarEventos(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "metodo nao permitido", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	rows, err := db.Query(ctx, `
		SELECT id, nome, ingressos_disponiveis
		FROM eventos
		ORDER BY id
	`)
	if err != nil {
		http.Error(w, "erro ao listar eventos", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	eventos := make([]Evento, 0, 1)

	for rows.Next() {
		var e Evento
		if err := rows.Scan(&e.ID, &e.Nome, &e.IngressosDisponiveis); err != nil {
			http.Error(w, "erro ao ler eventos", http.StatusInternalServerError)
			return
		}
		eventos = append(eventos, e)
	}

	if err := rows.Err(); err != nil {
		http.Error(w, "erro ao ler eventos", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(eventos)
}

func handleReservar(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "metodo nao permitido", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()

	var req ReservaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "payload invalido", http.StatusBadRequest)
		return
	}

	if req.EventoID <= 0 || req.UsuarioID <= 0 {
		http.Error(w, "payload invalido", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	tx, err := db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		http.Error(w, "erro ao iniciar transacao", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)

	var ingressos int
	err = tx.QueryRow(ctx, `
		SELECT ingressos_disponiveis
		FROM eventos
		WHERE id = $1
		FOR UPDATE
	`, req.EventoID).Scan(&ingressos)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "evento nao encontrado", http.StatusBadRequest)
			return
		}
		http.Error(w, "erro ao buscar evento", http.StatusInternalServerError)
		return
	}

	if ingressos <= 0 {
		http.Error(w, "nao ha ingressos disponiveis", http.StatusUnprocessableEntity)
		return
	}

	_, err = tx.Exec(ctx, `
		UPDATE eventos
		SET ingressos_disponiveis = ingressos_disponiveis - 1
		WHERE id = $1
	`, req.EventoID)
	if err != nil {
		http.Error(w, "erro ao atualizar ingressos", http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO reservas (evento_id, usuario_id)
		VALUES ($1, $2)
	`, req.EventoID, req.UsuarioID)
	if err != nil {
		http.Error(w, "erro ao criar reserva", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		http.Error(w, "erro ao confirmar reserva", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"mensagem": "reserva realizada com sucesso",
	})
}
