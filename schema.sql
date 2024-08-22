SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS EmbeddingType;
CREATE TABLE EmbeddingType
(
	id INTEGER  PRIMARY KEY,
	embedding_name VARCHAR(1024),
	vector_lenth INTEGER

);

INSERT INTO EmbeddingType VALUES
       (1, 'SFR-Mistral', 2048),
       (2, 'PubMedBERT', 768)
       ;

DROP TABLE IF EXISTS SourceDocument;
CREATE TABLE SourceDocument
(
	id VARCHAR(32) PRIMARY KEY,
	title_line TEXT,
	source_id VARCHAR(256),
	size INTEGER,
	pdf_text_location VARCHAR(1024)
);

DROP TABLE IF EXISTS DocumentParser;
CREATE TABLE DocumentParser
(
	id INTEGER PRIMARY KEY AUTO_INCREMENT,
	params JSON,
	parser_name VARCHAR(1024)
);

INSERT INTO DocumentParser VALUES
       (1, NULL, 'Nougat Tom')
       ;

DROP TABLE IF EXISTS ParsedDocument;
CREATE TABLE ParsedDocument
(
	id VARCHAR(32) PRIMARY KEY,
	document_id VARCHAR(32),
	parsed_text_location VARCHAR(1024),
	parser_id INTEGER,
	FOREIGN KEY (document_id) REFERENCES SourceDocument(id),
	FOREIGN KEY (parser_id) REFERENCES DocumentParser(id)
);

DROP TABLE IF EXISTS ChunkingScheme;
CREATE TABLE ChunkingScheme
(
	id INTEGER PRIMARY KEY,
	embedding_id INTEGER,
	params JSON,
	FOREIGN KEY (embedding_id) REFERENCES EmbeddingType(id)
);

DROP TABLE IF EXISTS DocumentChunk;
CREATE TABLE DocumentChunk
(
	chunk_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
	parsed_document_id VARCHAR(32),
	chunking_scheme_id INTEGER,
	chunk_text LONGTEXT,
	FOREIGN KEY (parsed_document_id) REFERENCES ParsedDocument(id)
);

DROP TABLE IF EXISTS ChunkVector;
CREATE TABLE ChunkVector
(
	chunk_id BIGINT UNSIGNED,
	embedding_id INTEGER,
	vector BLOB,
	FOREIGN KEY (chunk_id) REFERENCES DocumentChunk(chunk_id),
	FOREIGN KEY (embedding_id) REFERENCES EmbeddingType(id)
);
