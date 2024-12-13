from diagrams import Diagram, Cluster
from diagrams.aws.storage import S3
from diagrams.aws.database import RDS
from diagrams.generic.compute import Rack
from diagrams.generic.storage import Storage
from diagrams.aws.business import Workmail
from diagrams.custom import Custom
from diagrams.onprem.workflow import Airflow

# Initialize diagram
with Diagram("AirflowPipeline", show=False):
    
    # Data source and ingestion
    graph_api = Custom("Microsoft\nGraph API", "./images/GraphAPI.png")
    data_injector = Airflow("Data\nInjestor")

    # Email processing cluster
    with Cluster("Email Processing"):
        emails = [
            Workmail("Email\n(JSON Response)") for _ in range(3)
        ]
        attachments = [
            Storage("Attachments") for _ in range(3)
        ]
        
        # Connect emails to their attachments
        for i in range(3):
            emails[i] - attachments[i]

    # Feeder and LLM section
    feeder1 = Airflow("Feeder")
    feeder2 = Airflow("Feeder")
    filter = Custom("Profanity Filter", "./images/profanity.png")
    llm = Custom("LLM", "./images/OpenAI.png")
    category = Custom("Labels\n(Spam, Priority, Work, Advertising, etc.)", "./images/Description.png")

    # Database and storage
    postgres = RDS("PostgreSQL\nDatabase\n(AWS)")
    s3 = S3("S3 Bucket")

    # Attachment processing
    with Cluster("Attachment Processing"):
        text_attachments = Custom("Text-based attachments\n(txt, pdf, xlsx, etc.)", "./images/PDF_documents.png")
        image_attachments = Custom("Image attachments\n(jpg, png)", "./images/PNG.png")
        content_extraction = Airflow("Content Extraction\nService")
        
        # Data formats
        text_json = Custom("Text / JSON / MD", "./images/Text.png")
        csv_tables = Custom("CSV (Tables)", "./images/CSV.png")
        images = Custom("Images", "./images/PNG.png")
        
        # ML components
        multi_model_llm = Custom("Multi-Model\nLLM", "./images/OpenAI.png")
        image_description = Custom("Image description", "./images/Description.png")

    # Email processing components
    email_components = [Workmail("Email") for _ in range(3)]
    
    # Vector processing
    openai_embeddings = Custom("OpenAI\nEmbeddings", "./images/OpenAI.png")
    vector_store = Custom("Vector Store\n(Milvus)", "./images/Milvus.png")

    # Define connections
    graph_api >> data_injector >> emails
    
    # Connect feeders
    for email in emails:
        email >> feeder1
    
    feeder1 >> postgres
    filter >> feeder1
    feeder1 >> filter
    filter >> llm
    llm >> category
    category >> feeder1

    for i in range(3):
        attachments[i] >> feeder2
    
    # Connect to storage and processing
    feeder2 >> s3
    s3 >> text_attachments
    s3 >> image_attachments
    
    text_attachments >> content_extraction
    content_extraction >> text_json
    content_extraction >> csv_tables
    content_extraction >> images
    
    image_attachments >> multi_model_llm
    images >> multi_model_llm
    multi_model_llm >> image_description
    
    # Connect to vector store
    image_description >> openai_embeddings
    postgres >> email_components
    email_components >> openai_embeddings >> vector_store
    
    # Bottom vector store connection
    [text_json, csv_tables] >> openai_embeddings >> vector_store