#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <leveldb/c.h>
#include <limits.h>

// BackupOne
// Stima 40 TB spazio  -  40 M file - 4 M di cartelle/folders     46 bit  /  26 bit  /  22 bit
// 4 M folder == 128 MB dir-stat 3GB-fully counted KB-counted MB-counted  
// from 00000xxxx to 110000xxxx BYTE counted        upto 3GB
// from 11000xxxx to 111100xxxx KILOBYTE counted    upto 750GB
// from 111100xxx MEGABYTE COUNTED ()               upto 250TB

// FOLDER SORT-SEARCH-TABLE
// */FOLD/F123/F123.234.tabin

// each folder cache file-name-ids and file-ids
//
// FILE-ID => FILENAME-ID+FILESIZE+FILETIME => FILENAME
// 

// da FILENAME-ID TABELLA DEI FILENAME (40M x 50 byte==2GB) se si superano i 4GB si crea una seconda tabella dall'indice FNID(circa 80M di nomi)






int trace_flag = 1 ; // traccia cosa succede

void trace( char * text ) {
    if( trace_flag ) printf("%s\n",text);
}
void traceInt( char * text, int v ) {
    if( trace_flag ) printf("%s %d\n",text,v);
}
void traceString( char * text, char * v ) {
    if( trace_flag ) printf("%s %s\n",text,v);
}
void tracePtr( char * text, void *pt ) {
    if( trace_flag ) printf("%s %p\n",text,pt);
}



#define   G_TABLE_TEST      "table.bin"
#define   G_ERROR_CHANNEL   stdout
#define   G_TABLE_FILENAMES "table.filenames.bin"
#define   G_TABLE_FILENAMES_PTR "table.filenames.ptr.bin"

#define   G_TABLE_FILEPATHS "table.filepaths.bin"
#define   G_TABLE_FILEPATHS_PTR "table.filepaths.ptr.bin"

#define   G_db_PATH_name        "ldb_path"
#define   G_db_FILENAME_name    "ldb_filename"


FILE * GF_TABLE_FILENAMES_PTR = NULL ;
FILE * GF_TABLE_FILENAMES     = NULL ;
int    G_MAX_PATH_ID   =  0 ;
int    G_MAX_FILENAME_ID   =  0 ;

FILE * GF_TABLE_FILEPATHS     = NULL ;
FILE * GF_TABLE_FILEPATHS_PTR = NULL ;
FILE * fileIn = NULL ;   // ERRORE DA CORREGGERE NON DEVE ESISTERE


leveldb_t * G_db_path ;
leveldb_t * G_db_filename ;

void errorString( const char * errorText, const char * _errorString ) {
    fprintf( G_ERROR_CHANNEL, errorText, _errorString );
    fprintf( G_ERROR_CHANNEL, "\n" ) ;
    exit(-1) ;
}

void errorInt( const char * errorText, int _errorInt ) {
    fprintf( G_ERROR_CHANNEL, errorText, _errorInt );
    fprintf( G_ERROR_CHANNEL, "\n" ) ;
    exit(-1) ;
}

int table_get_filename_ptr( int filename_id ) {
  fseek( GF_TABLE_FILENAMES_PTR, filename_id, SEEK_SET ) ;
  unsigned int filename_ptr = -1 ;
  fread( &filename_ptr, 1, 4, GF_TABLE_FILENAMES_PTR ) ;
}

unsigned char * table_get_filename( int filename_id ) {
  fseek( GF_TABLE_FILENAMES_PTR, filename_id, SEEK_SET ) ;
  unsigned int filename_ptr = -1 ;
  unsigned int filename_next_ptr = -1 ;
  fread( &filename_ptr, 1, 4, GF_TABLE_FILENAMES_PTR ) ;
  fread( &filename_next_ptr, 1, 4, GF_TABLE_FILENAMES_PTR ) ;
  int filename_len = filename_next_ptr-filename_ptr ;
  unsigned char * filename ;
  filename = malloc( filename_len ) ;
  fseek( GF_TABLE_FILENAMES, filename_ptr, SEEK_SET ) ;
  fread( filename, 1, filename_len, GF_TABLE_FILENAMES ) ;
  return filename ;
}

int table_save_filename( int filename_id, unsigned char * filename ) {
  int filename_len = strlen( filename ) ;

  fseek( fileIn, 0L, SEEK_END ) ;
  long filesize = ftell( fileIn ) ;
  
  // TODO: check if filesize+filename_len > UINT_MAX

  fclose( GF_TABLE_FILENAMES ) ;
  GF_TABLE_FILENAMES = fopen( G_TABLE_FILENAMES, "ab" ) ;
  fwrite( filename, 1, filename_len+1, GF_TABLE_FILENAMES ) ;

  long filesize_after = ftell( GF_TABLE_FILENAMES ) ;
  
}

int table_split(unsigned char * table, int tableSize, unsigned char * table_path ){
    
}

// meglio file da 64KB pieni a meta
// prevedere la tabella indice per ricerca veloce <nome-secondo> <nome-terzo> <nome-ultimo> 
int table_findInsertPosition( int id, unsigned char * name, unsigned char * table, int tableSize ) {
  // deve restituire solo la POSIZIONE dell INSERT poi si sovrascriverà solo la parte da POSIZIONE alla fine
  int scorri = 4 ;
  int scorriname = 0 ;
  int qualcosa = 1 ;

  if( tableSize == 5 ) {
    return 0 ; // vuoto
  }
  
  while( qualcosa ) {
    traceInt("scorriname",scorriname);
    traceInt("table[scorri]", table[scorri]);
    int ultimoScorri = scorri ;
    while( (name[scorriname] == table[scorri] ) && name[scorriname] ) { scorri++; scorriname++; traceInt("scorriname++",scorriname); }
    traceInt("scorriname",scorriname);
    traceInt("table[scorri]", table[scorri]);

    if( name[scorriname] ) {
      // nomi diversi
      traceInt( "nomi diversi", name[scorriname] ) ;
      if( table[scorri] < name[scorriname] ) {
        // parola minore, scorriamo fino alla prossima parola
        scorriname = 0 ;
        while( table[scorri] ) { scorri++; traceInt("scorri++",scorri); }
        // scorri/NULL può essere in posizione 0 1 2 3 deve avanzare di 4 3 2 1 + [id-4 byte]
        scorri += 8 - (scorri&3) ;
        traceInt("scorri jump",scorri);
        if( ! table[scorri] ) {
          // fine lista - la lista finisce con 5 zero [id:0.0.0.0]+[empty-string:0]
          traceInt("fine lista - la lista finisce con 5 zero",scorri);
          if( tableSize != scorri+1 ) {
            // ERRORE nella lunghezza tabella
            traceInt("ERRORE nella lunghezza tabella",tableSize);
            errorInt("ERRORE nella lunghezza tabella : %d", tableSize);
            return 0 ;
          }
          return scorri-4 ;
        }
      } else if( table[scorri] > name[scorriname] ) {
        // parola maggiore, restituiamo inizio parola
        traceInt("parola maggiore, restituiamo inizio parola",ultimoScorri-4);
        return ultimoScorri-4 ;
      } else {
        // parola già presente
        return -1 -ultimoScorri+4 ;
      }
    } else {
        // finito name    aka    name[scorriname] == 0
        traceInt("finito name    aka    name[scorriname] == 0 :", scorriname ) ;
        if( ! table[scorri] ) {
            // parola già presente
            return -1 -ultimoScorri+4 ;
        }
        errorInt("verificare : %d", tableSize);
    }
  }
}

char zero_padding[4] = {0,0,0,0} ;

int table_save( unsigned char * table_path, unsigned char * table, int tableSize, int pos, int name_id, unsigned char * name  ) {
  FILE * fileOut = fopen( table_path, "wb" ) ;
  if( fileOut==NULL ) { errorString( "table_save: cannot open table_path:",table_path); return 0 ; }

  fwrite( table, 1, pos, fileOut ) ;

  int name_len = strlen( name )+1 ;
  name_id = -name_id ;
  fwrite( &name_id, 4, 1, fileOut ) ;
  fwrite( name, 1, name_len, fileOut ) ;
  
  int name_pad = (4-(name_len & 3))&3 ; //    5==3   6==2   7==1   8 == 0
  fwrite( zero_padding, 1, name_pad, fileOut ) ;
  
  fwrite( table+pos, 1, tableSize-pos, fileOut ) ;
  fclose(fileOut);
  
  return tableSize+4+name_len+name_pad ;
}

unsigned char * table_load(  const unsigned char * table_path, int * tableSize  ) {
  FILE * fileIn = fopen( table_path, "rb" ) ;
  if( fileIn==NULL ) { errorString( "table_load: cannot open table_path: %s",table_path); return NULL ; }
  fseek( fileIn, 0L, SEEK_END ) ;
  long filesize = ftell( fileIn ) ;
  traceInt("filesize",filesize);
    if( filesize >= INT_MAX ) { errorString( "table_load: table bigger than 2 GB : %s",table_path); return NULL ; }
  fseek( fileIn, 0L, SEEK_SET ) ;
  char * buffer = malloc( filesize ) ;
  fread( buffer, 1, filesize, fileIn ) ;
  *tableSize = filesize ;
  return buffer ;
}


int fn_write_new_filename( int filename_id, unsigned char * filename ) {
  int filename_len = strlen( filename ) ;

  fseek( GF_TABLE_FILENAMES_PTR, 0L, SEEK_END ) ;
  long tablesize = ftell( GF_TABLE_FILENAMES ) ;
  
  // TODO: check if filesize+filename_len > UINT_MAX

  // fclose( GF_TABLE_FILENAMES ) ; GF_TABLE_FILENAMES = fopen( GF_TABLE_FILENAMES, "ab" ) ;
  fwrite( filename, 1, filename_len+1, GF_TABLE_FILENAMES ) ;

  long filesize_after = ftell( GF_TABLE_FILENAMES ) ;
  int fsa = filesize_after ;
  fwrite( &fsa, 1, 4, GF_TABLE_FILENAMES_PTR ) ;

}


int fn_write_new_path( int path_id, unsigned char * path ) {
  int path_len = strlen( path ) ;

  fseek( GF_TABLE_FILEPATHS_PTR, 0L, SEEK_END ) ;
  long tablesize = ftell( GF_TABLE_FILEPATHS ) ;
  
  // TODO: check if filesize+filename_len > UINT_MAX

  // fclose( GF_TABLE_FILENAMES ) ; GF_TABLE_FILENAMES = fopen( GF_TABLE_FILENAMES, "ab" ) ;
  fwrite( path, 1, path_len+1, GF_TABLE_FILEPATHS ) ;

  long filesize_after = ftell( GF_TABLE_FILEPATHS ) ;
  int fsa = filesize_after ;
  fwrite( &fsa, 1, 4, GF_TABLE_FILEPATHS_PTR ) ;

}

// get or create PATH ID
int fn_set_path_id( unsigned char * filepath, int path_id ) {
  
    char *err = NULL;

      leveldb_writeoptions_t *woptions;

      woptions = leveldb_writeoptions_create();
      leveldb_put(G_db_path, woptions, filepath, strlen(filepath), (char *) &path_id, 4, &err);

      if (err != NULL) {
        fprintf(stderr, "fn_set_path_id: Write fail.\n");
        return(1);
      }
      leveldb_free(err); err = NULL;

}

// get or create FILENAME ID
int fn_get_filename_id( unsigned char * filename ) {
  
    leveldb_readoptions_t *roptions;
    char *err = NULL;
    unsigned char *read;
    size_t read_len;


    roptions = leveldb_readoptions_create();
    read = leveldb_get(G_db_filename, roptions, filename, strlen(filename), &read_len, &err);

    if (err != NULL) {
      fprintf( G_ERROR_CHANNEL, "fn_get_filename_id: Read fail. (%d,%s)\n", err, err );
      return(-1);
    }
    leveldb_free(err); err = NULL;

    if( read == NULL ) {
      // FILENAME NOT FOUND, CREATE IT
      int filename_id = ++G_MAX_FILENAME_ID ;

      leveldb_writeoptions_t *woptions;

      woptions = leveldb_writeoptions_create();
      leveldb_put(G_db_filename, woptions, filename, strlen(filename), (char *) &filename_id, 4, &err);

      if (err != NULL) {
        fprintf(stderr, "fn_get_filename_id: Write fail.\n");
        return(1);
      }
      leveldb_free(err); err = NULL;

      fn_write_new_filename( filename_id, filename ) ;

      return filename_id ;
    }

    //printf("%s: %s\n", keyname, read);

    return *( (int *) read ) ;

}

// get or create PATH ID
int fn_get_path_id( unsigned char * filepath ) {
  
    leveldb_readoptions_t *roptions;
    char *err = NULL;
    unsigned char *read;
    size_t read_len;


    roptions = leveldb_readoptions_create();
    read = leveldb_get(G_db_path, roptions, filepath, strlen(filepath), &read_len, &err);

    if (err != NULL) {
      fprintf( G_ERROR_CHANNEL, "fn_get_path_id: Read fail. (%d,%s)\n", err, err );
      return(-1);
    }
    leveldb_free(err); err = NULL;

    if( read == NULL ) {
      // PATH NOT FOUND, CREATE IT
      int path_id = ++G_MAX_PATH_ID ;

      leveldb_writeoptions_t *woptions;

      woptions = leveldb_writeoptions_create();
      leveldb_put(G_db_path, woptions, filepath, strlen(filepath), (char *) &path_id, 4, &err);

      if (err != NULL) {
        fprintf(stderr, "Write fail.\n");
        return(1);
      }
      leveldb_free(err); err = NULL;

      fn_write_new_path( path_id, filepath ) ;

      return path_id ;
    }

    //printf("%s: %s\n", keyname, read);

    return *( (int *) read ) ;

}

int main( int argc, char**argv ) {

    leveldb_options_t *options;
    char *err = NULL;

    /******************************************/
    /* OPEN */

    options = leveldb_options_create();
    leveldb_options_set_create_if_missing(options, 1);
    G_db_path = leveldb_open(options, G_db_PATH_name, &err);

    if (err != NULL) {
      fprintf(stderr, "Open fail.\n");
      return(1);
    }

    /* reset error var */
    leveldb_free(err); err = NULL;


    G_MAX_PATH_ID = fn_get_path_id( "G_MAX_PATH_ID" ) ;









  GF_TABLE_FILEPATHS     = fopen( G_TABLE_FILEPATHS    , "r+" ) ;
  GF_TABLE_FILEPATHS_PTR = fopen( G_TABLE_FILEPATHS_PTR, "r+" ) ;
  if(    GF_TABLE_FILEPATHS == NULL    ) {    errorString( "CANNOT OPEN G_TABLE_FILEPATHS: %s", G_TABLE_FILEPATHS ) ;     }
  if(    GF_TABLE_FILEPATHS_PTR == NULL    ) {    errorString( "CANNOT OPEN G_TABLE_FILEPATHS_PTR: %s", G_TABLE_FILEPATHS_PTR ) ;     }
  fseek( GF_TABLE_FILEPATHS    , 0L, SEEK_END ) ;
  fseek( GF_TABLE_FILEPATHS_PTR, 0L, SEEK_END ) ;

  traceInt( "argc: ", argc ) ;
  traceInt( "sizeof(long): ", sizeof(long) ) ;

  if( argc > 1 ) {
    int pid = fn_get_path_id( argv[1] ) ;
    printf(   "path:  %d  %s\n\n", pid, argv[1]  ) ;
  } else {
    printf(   "use : prog   path-name\n\n" );
  }

  fn_set_path_id( "G_MAX_PATH_ID", G_MAX_PATH_ID ) ;

  return 0 ; 
  
  int testa = -65537*256 ;

  printf( "sizeof testa: %d\n", sizeof(testa) ) ;
  printf( "big testa: %d\n", -testa ) ;

  char * charp = (char *) &testa ;
  printf( "testa : %d %d %d %d\n", charp[0], charp[1], charp[2], charp[3] ) ;
/*
  char * tabella = calloc( 5, 1 );
  int tabella_size = 5 ;
*/
  char * tabella = NULL;
  int tabella_size = 0 ;

  tabella = table_load( G_TABLE_TEST, &tabella_size ) ;

  char * nome = "provo la tacheta" ;
  int nome_id = 19 ;
  
  traceString("table_findInsertPosition()", nome) ;
  int r = table_findInsertPosition( nome_id, nome, tabella, tabella_size ) ;
  printf( "table_findInsertPosition : %d \n", r ) ;
  if( r < 0 )  errorInt( "ERRORE table_findInsertPosition : parola gia presente : %d", r );
  
  table_save( G_TABLE_TEST, tabella, tabella_size, r, nome_id, nome ) ;
}



