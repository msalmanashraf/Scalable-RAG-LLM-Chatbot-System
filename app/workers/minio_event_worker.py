import pika
import os
from minio import Minio
from minio.error import S3Error
from minio.notificationconfig import (NotificationConfig, QueueConfig)
import urllib.parse
import json
from pika.exceptions import ChannelClosedByBroker
import PyPDF2
import re
from typing import List, Dict
import tiktoken
import unicodedata
from dataclasses import dataclass
import spacy
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DocumentChunker:
    """
    A class for splitting documents into manageable chunks for processing.
    
    This chunker uses token counting to ensure chunks stay within size limits
    while attempting to preserve semantic boundaries like sentences.
    """
    
    def __init__(self, max_chunk_size: int = 2000, chunk_overlap: int = 200):
        """
        Initialize the document chunker with configurable parameters.
        
        Args:
            max_chunk_size: Maximum number of characters per chunk
            chunk_overlap: Number of characters to overlap between chunks
        """
        self.max_chunk_size = max_chunk_size
        self.chunk_overlap = chunk_overlap
        # Initialize tokenizer for Mistral (using cl100k_base as it's compatible)
        self.tokenizer = tiktoken.get_encoding("cl100k_base")
        
    def clean_text(self, text: str) -> str:
        """
        Clean and normalize text from PDF.
        
        Args:
            text: Raw text extracted from PDF
            
        Returns:
            Cleaned and normalized text
        """
        # Remove multiple newlines
        text = re.sub(r'\n{3,}', '\n\n', text)
        # Remove multiple spaces
        text = re.sub(r' +', ' ', text)
        # Remove Unicode control characters
        text = ''.join(char for char in text if not unicodedata.category(char).startswith('C'))
        return text.strip()

    def extract_pdf_text(self, file_path: str) -> str:
        """
        Extract text from PDF with better formatting preservation.
        
        Args:
            file_path: Path to the PDF file
            
        Returns:
            Extracted and cleaned text content
        """
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text_content = []
                
                for page in pdf_reader.pages:
                    text = page.extract_text()
                    if text:
                        # Clean the extracted text
                        text = self.clean_text(text)
                        text_content.append(text)
                
                return '\n\n'.join(text_content)
        except Exception as e:
            logger.error(f"Error extracting PDF text: {e}")
            return ""

    def get_token_count(self, text: str) -> int:
        """
        Get the number of tokens in a text string.
        
        Args:
            text: Text to count tokens for
            
        Returns:
            Number of tokens in the text
        """
        return len(self.tokenizer.encode(text))

    def create_chunks(self, text: str, source_file: str) -> List[Dict[str, str]]:
        """
        Create overlapping chunks from text that respect sentence boundaries.
        
        Args:
            text: The text to chunk
            source_file: The source file name for reference
            
        Returns:
            List of dictionaries containing chunk information
        """
        # Split text into sentences
        sentences = re.split(r'(?<=[.!?])\s+', text)
        chunks = []
        current_chunk = []
        current_size = 0
        
        for sentence in sentences:
            sentence_tokens = self.get_token_count(sentence)
            
            # If a single sentence is too large, split it
            if sentence_tokens > self.max_chunk_size:
                if current_chunk:
                    chunks.append(' '.join(current_chunk))
                    current_chunk = []
                    current_size = 0
                
                # Split long sentence by comma or other natural breaks
                sub_sentences = re.split(r'(?<=[,;:])\s+', sentence)
                for sub_sentence in sub_sentences:
                    if self.get_token_count(sub_sentence) > self.max_chunk_size:
                        # If still too large, force split
                        words = sub_sentence.split()
                        temp_chunk = []
                        temp_size = 0
                        for word in words:
                            word_tokens = self.get_token_count(word)
                            if temp_size + word_tokens > self.max_chunk_size:
                                chunks.append(' '.join(temp_chunk))
                                temp_chunk = [word]
                                temp_size = word_tokens
                            else:
                                temp_chunk.append(word)
                                temp_size += word_tokens
                        if temp_chunk:
                            chunks.append(' '.join(temp_chunk))
                    else:
                        if current_size + self.get_token_count(sub_sentence) > self.max_chunk_size:
                            chunks.append(' '.join(current_chunk))
                            current_chunk = [sub_sentence]
                            current_size = self.get_token_count(sub_sentence)
                        else:
                            current_chunk.append(sub_sentence)
                            current_size += self.get_token_count(sub_sentence)
            else:
                if current_size + sentence_tokens > self.max_chunk_size:
                    chunks.append(' '.join(current_chunk))
                    current_chunk = [sentence]
                    current_size = sentence_tokens
                else:
                    current_chunk.append(sentence)
                    current_size += sentence_tokens
        
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        
        # Create final chunk objects with overlap
        chunk_objects = []
        for i, chunk in enumerate(chunks):
            # Add overlap from previous chunk
            if i > 0:
                prev_chunk = chunks[i-1]
                prev_sentences = re.split(r'(?<=[.!?])\s+', prev_chunk)
                overlap_text = ' '.join(prev_sentences[-3:])  # Take last 3 sentences for overlap
            else:
                overlap_text = ""
            
            chunk_text = overlap_text + " " + chunk if overlap_text else chunk
            
            chunk_objects.append({
                "text": chunk_text,
                "chunk_id": f"{source_file}_chunk_{i}",
                "source": source_file,
                "token_count": self.get_token_count(chunk_text)
            })
        
        return chunk_objects

@dataclass
class Chunk:
    """
    Data class representing a document chunk.
    
    Attributes:
        text: The text content of the chunk
        chunk_id: Unique identifier for the chunk
        source: Source document filename
    """
    text: str
    chunk_id: str
    source: str
    
class SentenceTransformerChunker:
    """
    A document chunker optimized for sentence transformer models.
    
    This chunker attempts to create semantically meaningful chunks
    that fit within the input size constraints of transformer models
    while preserving the semantic coherence of the content.
    """
    
    def __init__(
        self,
        max_chunk_size: int = 512,  # Typical maximum for most sentence transformers
        min_chunk_size: int = 100,   # Minimum to ensure meaningful semantic content
        nlp=None
    ):
        """
        Initialize chunker optimized for sentence transformers.
        
        Args:
            max_chunk_size: Maximum tokens per chunk (default 512 for most sentence transformers)
            min_chunk_size: Minimum tokens per chunk to maintain semantic meaning
            nlp: Optional spacy model (will load en_core_web_sm if None)
        """
        self.max_chunk_size = max_chunk_size
        self.min_chunk_size = min_chunk_size
        self.nlp = nlp or spacy.load("en_core_web_sm")
        logger.info(f"Initialized SentenceTransformerChunker with max_size={max_chunk_size}, min_size={min_chunk_size}")
        
    def clean_text(self, text: str) -> str:
        """
        Clean and normalize text while preserving semantic structure.
        
        Args:
            text: Raw text to clean
            
        Returns:
            Cleaned and normalized text
        """
        # Remove excessive whitespace while preserving paragraph breaks
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' +', ' ', text)
        
        # Remove Unicode control characters but keep essential formatting
        text = ''.join(char for char in text 
                      if not unicodedata.category(char).startswith('C') 
                      or char in ['\n', '\t'])
        
        # Ensure proper sentence spacing
        text = re.sub(r'([.!?])\s*(\w)', r'\1 \2', text)
        
        return text.strip()

    def extract_pdf_text(self, file_path: str) -> str:
        """
        Extract text from PDF preserving document structure.
        
        Args:
            file_path: Path to the PDF file
            
        Returns:
            Extracted and cleaned text content with preserved paragraph structure
        """
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text_content = []
                
                for page in pdf_reader.pages:
                    text = page.extract_text()
                    if text:
                        # Preserve paragraph structure
                        paragraphs = re.split(r'\n\s*\n', text)
                        cleaned_paragraphs = [self.clean_text(p) for p in paragraphs if p.strip()]
                        text_content.extend(cleaned_paragraphs)
                
                logger.info(f"Extracted {len(pdf_reader.pages)} pages from PDF: {file_path}")
                return '\n\n'.join(text_content)
        except Exception as e:
            logger.error(f"Error extracting PDF text: {e}")
            return ""

    def create_chunks(self, text: str, source_file: str) -> List[Dict[str, str]]:
        """
        Create chunks optimized for sentence transformers by maintaining semantic coherence.
        
        Args:
            text: Text to chunk
            source_file: Source filename for reference
            
        Returns:
            List of chunk objects with semantically coherent content
        """
        # Process text with spaCy to get semantic units
        doc = self.nlp(text)
        
        chunks = []
        current_chunk = []
        current_size = 0
        
        # Group sentences into semantic units (paragraphs/sections)
        for sent in doc.sents:
            sent_text = sent.text.strip()
            sent_size = len(sent)
            
            # Handle very long sentences
            if sent_size > self.max_chunk_size:
                # If we have accumulated content, save it first
                if current_chunk:
                    chunks.append(Chunk(
                        text=' '.join(current_chunk),
                        chunk_id=f"{source_file}_chunk_{len(chunks)}",
                        source=source_file
                    ))
                    current_chunk = []
                    current_size = 0
                
                # Split long sentence at clause boundaries
                clauses = [span.text.strip() for span in sent.noun_chunks]
                temp_chunk = []
                temp_size = 0
                
                for clause in clauses:
                    clause_size = len(self.nlp(clause))
                    if temp_size + clause_size > self.max_chunk_size:
                        if temp_chunk:  # Save accumulated clauses
                            chunks.append(Chunk(
                                text=' '.join(temp_chunk),
                                chunk_id=f"{source_file}_chunk_{len(chunks)}",
                                source=source_file
                            ))
                        temp_chunk = [clause]
                        temp_size = clause_size
                    else:
                        temp_chunk.append(clause)
                        temp_size += clause_size
                
                if temp_chunk:  # Save any remaining clauses
                    chunks.append(Chunk(
                        text=' '.join(temp_chunk),
                        chunk_id=f"{source_file}_chunk_{len(chunks)}",
                        source=source_file
                    ))
                
            # Normal sentence handling
            elif current_size + sent_size > self.max_chunk_size:
                # Save current chunk if it meets minimum size
                if current_size >= self.min_chunk_size:
                    chunks.append(Chunk(
                        text=' '.join(current_chunk),
                        chunk_id=f"{source_file}_chunk_{len(chunks)}",
                        source=source_file
                    ))
                    current_chunk = [sent_text]
                    current_size = sent_size
                else:
                    # If below minimum size, try to find a better break point
                    next_size = current_size + sent_size
                    if next_size <= self.max_chunk_size * 1.1:  # Allow slight overflow
                        current_chunk.append(sent_text)
                        current_size = next_size
                    else:
                        chunks.append(Chunk(
                            text=' '.join(current_chunk),
                            chunk_id=f"{source_file}_chunk_{len(chunks)}",
                            source=source_file
                        ))
                        current_chunk = [sent_text]
                        current_size = sent_size
            else:
                current_chunk.append(sent_text)
                current_size += sent_size
        
        # Add final chunk if it exists
        if current_chunk and current_size >= self.min_chunk_size:
            chunks.append(Chunk(
                text=' '.join(current_chunk),
                chunk_id=f"{source_file}_chunk_{len(chunks)}",
                source=source_file
            ))
        
        logger.info(f"Created {len(chunks)} chunks from document: {source_file}")
        return [chunk.__dict__ for chunk in chunks]  # Convert to dict for JSON serialization

class MinioEventHandler:
    """
    A handler for MinIO object storage events that processes PDF documents.
    
    This class sets up connections to MinIO and RabbitMQ, configures event
    notifications, and processes PDF files by chunking them and sending
    the chunks to a message queue for further processing.
    """
    
    def __init__(self, config_dir: str):
        """
        Initialize the MinIO event handler.
        
        Args:
            config_dir: Directory containing configuration files
        """
        self.config_dir = config_dir
        self.chunker = SentenceTransformerChunker()
        logger.info(f"Initializing MinioEventHandler with config directory: {config_dir}")
        self.setup_minio_client()
        self.setup_minio_notification()
        self.setup_rabbitmq()

    def setup_minio_client(self):
        """
        Set up the MinIO client connection using configuration file.
        """
        config_path = os.path.join(self.config_dir, 'minio_config.json')
        with open(config_path) as f:
            config = json.load(f)
        
        self.minio_client = Minio(
            config['endpoint'],
            access_key=config['username'],
            secret_key=config['password'],
            secure=False
        )
        logger.info(f"Connected to MinIO endpoint: {config['endpoint']}")

    def setup_minio_notification(self):
        """
        Configure MinIO to send notifications when objects are created.
        """
        config_path = os.path.join(self.config_dir, 'minio_config.json')
        with open(config_path) as f:
            config = json.load(f)
        
    
        bucket_name = config['bucket_name']

        queue_arn = config['queue_arn']
        events = ["s3:ObjectCreated:*"]  # Events for new object creation
    
        # Create QueueConfig for MinIO notifications
        queue_config = QueueConfig(queue=queue_arn, events=events)

        queue_config_list = [queue_config]

        config = NotificationConfig(queue_config_list)

        # Apply the notification configuration to the bucket
        try:
            self.minio_client.set_bucket_notification(bucket_name, config)
            logger.info(f"Successfully configured event notification for bucket {bucket_name}")
        except S3Error as err:
            logger.error(f"Error configuring MinIO event notification: {err}")

    def setup_rabbitmq(self):
        """
        Set up the RabbitMQ connection for consuming MinIO events
        and publishing document chunks.
        """
        config_path = os.path.join(self.config_dir, 'rabbitmq_config.json')
        with open(config_path) as f:
            config = json.load(f)
        
        credentials = pika.PlainCredentials(
            config['rabbitmq']['username'],
            config['rabbitmq']['password']
        )
        parameters = pika.ConnectionParameters(
            config['rabbitmq']['host'],
            credentials=credentials
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        logger.info(f"Connected to RabbitMQ host: {config['rabbitmq']['host']}")

    def handle_message(self, ch, method, properties, body):
        """
        Handle incoming MinIO events by processing documents and creating chunks.
        
        Args:
            ch: Channel object
            method: Method frame
            properties: Properties
            body: Message body containing event data
        """
        try:
            event_data = json.loads(body)
            raw_file_name = event_data["Records"][0]["s3"]["object"]["key"]
            file_name = urllib.parse.unquote_plus(raw_file_name)
            
            logger.info(f"Processing new file event: {file_name}")
            
            # Download file from MinIO
            local_path = f"/tmp/{os.path.basename(file_name)}"
            self.minio_client.fget_object("faq-documents", file_name, local_path)
            logger.info(f"Downloaded file to {local_path}")
            
            # Extract and chunk text
            text = self.chunker.extract_pdf_text(local_path)
            chunks = self.chunker.create_chunks(text, file_name)
            
            # Log chunking results
            logger.info(f"Created {len(chunks)} chunks from {file_name}")
            # logger.debug(f"Average token count per chunk: {sum(c['token_count'] for c in chunks)/len(chunks):.2f}")
            
            # Publish chunks to queue
            for chunk in chunks:
                self.channel.basic_publish(
                    exchange='',
                    routing_key='document_chunks_queue',
                    body=json.dumps(chunk)
                )
            logger.info(f"Published {len(chunks)} chunks to document_chunks_queue")
            
            # Cleanup
            os.remove(local_path)
            logger.info(f"Cleaned up temporary file: {local_path}")
            
        except Exception as e:
            logger.error(f"Error processing file: {e}", exc_info=True)


def main():
    """
    Main entry point for the MinIO event handler service.
    
    Sets up configuration directories and starts the event listener
    to process documents as they are uploaded to MinIO.
    """
    parent_dir = os.path.dirname(os.path.abspath(__file__))
    config_dir = os.path.join(parent_dir, '..', 'config')
    
    logger.info("Starting MinIO event handler service")
    handler = MinioEventHandler(config_dir)
    
    # Set up RabbitMQ consumer
    # handler.channel.queue_declare(queue='minio_events_queue')
    handler.channel.basic_consume(
        queue='minio_events_queue',
        on_message_callback=handler.handle_message,
        auto_ack=True
    )
    
    logger.info("Waiting for MinIO events...")
    handler.channel.start_consuming()

if __name__ == "__main__":
    main()