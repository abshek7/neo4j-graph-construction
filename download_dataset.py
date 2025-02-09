import kaggle

def download_tmdb_dataset():
    kaggle.api.dataset_download_files(
        'asaniczka/tmdb-movies-dataset-2023-930k-movies',
        path='.',
        unzip=True
    )

if __name__ == "__main__":
    download_tmdb_dataset() 