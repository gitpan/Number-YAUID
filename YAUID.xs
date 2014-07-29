//
//
//  Created by Alexander Borisov on 22.07.14.
//  Copyright (c) 2014 Alexander Borisov. All rights reserved.
//

#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include <yauid.h>

#ifdef ENVIRONMENT32
#error Only 64 bit system
#else

unsigned long yauid_get_inc_id(hkey_t key)
{
    key <<= (BIT_LIMIT_TIMESTAMP + BIT_LIMIT_NODE);
    key >>= (BIT_LIMIT - BIT_LIMIT_INC);
    
    return (unsigned long)(key);
}

unsigned long yauid_get_node_id(hkey_t key)
{
    key <<= BIT_LIMIT_TIMESTAMP;
    key >>= (BIT_LIMIT - BIT_LIMIT_NODE);
    
    return (unsigned long)(key);
}

unsigned long yauid_get_timestamp(hkey_t key)
{
    key >>= (BIT_LIMIT_NODE + BIT_LIMIT_INC);
    
    return (unsigned long)(key);
}

unsigned long long int yauid_get_max_inc()
{
    return NUMBER_LIMIT;
}

unsigned long long int yauid_get_max_node_id()
{
    return NUMBER_LIMIT_NODE;
}

unsigned long long int yauid_get_max_timestamp()
{
    return NUMBER_LIMIT_TIMESTAMP;
}

hkey_t yauid_get_key(yauid* yaobj)
{
    hkey_t key = (hkey_t)(0);
    unsigned int count = 0;
	
    for(;;)
    {
        if((key = yauid_get_key_once(yaobj)) == (hkey_t)(0))
        {
            if(yaobj->error == YAUID_ERROR_KEYS_ENDED)
            {
                count++;
                
                if(yaobj->try_count && count >= yaobj->try_count)
                {
                    yaobj->error = YAUID_ERROR_TRY_COUNT_KEY;
                    break;
                }
                
                usleep(yaobj->sleep_usec);
                continue;
            }
        }
        
        break;
    }
    
    return key;
}

hkey_t yauid_get_key_once(yauid* yaobj)
{
    hkey_t key = (hkey_t)(0), tmp = (hkey_t)(1), ltime = (hkey_t)(0);
    
    yaobj->error = YAUID_OK;
    
    if(flock(yaobj->i_lockfile, LOCK_EX) == -1)
    {
        yaobj->error = YAUID_ERROR_FILE_LOCK;
        return (hkey_t)(0);
    }
    
    if(fseek(yaobj->h_lockfile, 0, SEEK_SET) != 0)
    {
        yaobj->error = YAUID_ERROR_FILE_SEEK;
        return (hkey_t)(0);
    }
    
    if(fread((void *)(&key), sizeof(hkey_t), 1, yaobj->h_lockfile) != 1)
    {
        if(fseek(yaobj->h_lockfile, 0L, SEEK_END) != 0)
        {
            yaobj->error = YAUID_ERROR_FILE_SEEK;
            return (hkey_t)(0);
        }
        
        long h_size = ftell(yaobj->h_lockfile);
        if(h_size > 0)
        {
            yaobj->error = YAUID_ERROR_READ_KEY;
            return (hkey_t)(0);
        }
        
        if(fseek(yaobj->h_lockfile, 0, SEEK_SET) != 0)
        {
            yaobj->error = YAUID_ERROR_FILE_SEEK;
            return (hkey_t)(0);
        }
    }
    
    ltime = time(NULL);
    
    if(key)
    {
        tmp = key >> (BIT_LIMIT_NODE + BIT_LIMIT_INC);
        key <<= (BIT_LIMIT_TIMESTAMP + BIT_LIMIT_NODE);
        key >>= (BIT_LIMIT - BIT_LIMIT_INC);
        
        key++;
        
        if(tmp == ltime)
        {
            if(key > (hkey_t)(NUMBER_LIMIT))
            {
                flock(yaobj->i_lockfile, LOCK_UN);
                
                yaobj->error = YAUID_ERROR_KEYS_ENDED;
                return (hkey_t)(0);
            }
            
            tmp = key;
        }
        else
            tmp = (hkey_t)(1);
    }
    
    key = ltime;
    key <<= BIT_LIMIT_NODE;
    
    key |= yaobj->node_id;
    key <<= BIT_LIMIT_INC;
    
    key |= tmp;
    
    if(fseek(yaobj->h_lockfile, 0, SEEK_SET) != 0)
    {
        yaobj->error = YAUID_ERROR_FILE_SEEK;
        return (hkey_t)(0);
    }
    
    if(fwrite((const void *)(&key), sizeof(hkey_t), 1, yaobj->h_lockfile) != 1)
    {
        yaobj->error = YAUID_ERROR_WRITE_KEY;
        return (hkey_t)(0);
    }
    
    if(fflush(yaobj->h_lockfile) != 0)
    {
        yaobj->error = YAUID_ERROR_FLUSH_KEY;
        return (hkey_t)(0);
    }
    
    if(flock(yaobj->i_lockfile, LOCK_UN) == -1)
    {
        yaobj->error = YAUID_ERROR_FILE_LOCK;
        return (hkey_t)(0);
    }
    
	yaobj->error = YAUID_OK;
	
    return key;
}

yauid * yauid_init(const char *filepath_key, const char *filepath_node_id)
{
    yauid* yaobj = (yauid *)malloc(sizeof(yauid));
    
    if(yaobj)
    {
        yaobj->node_id    = 0;
        yaobj->error      = YAUID_OK;
        yaobj->c_lockfile = filepath_key;
        yaobj->i_lockfile = 0;
        yaobj->h_lockfile = NULL;
        yaobj->try_count  = 0;
        yaobj->sleep_usec = (useconds_t)(35000L);
        
        if(filepath_node_id != NULL)
        {
            if(access( filepath_node_id, F_OK ) != -1)
            {
                FILE* h_node_id;
                if((h_node_id = fopen(filepath_node_id, "rb")))
                {
                    fseek(h_node_id, 0L, SEEK_END);
                    
                    long h_size = ftell(h_node_id);
                    if(h_size <= 0)
                    {
                        fclose(h_node_id);
                        yaobj->error = YAUID_ERROR_FILE_NODE_ID;
                        return yaobj;
                    }
                    
                    fseek(h_node_id, 0L, SEEK_SET);
                    
                    char *text = (char *)malloc(sizeof(char) * (h_size + 1));
                    if(text == NULL)
                    {
                        fclose(h_node_id);
                        yaobj->error = YAUID_ERROR_FILE_NODE_MEM;
                        return yaobj;
                    }
                    
                    fread(text, sizeof(char), h_size, h_node_id);
                    fclose(h_node_id);
                    
                    long i = 0;
                    for(i = 0; i < h_size; i++)
                    {
                        if(text[i] >= '0' && text[i] <= '9')
                            yaobj->node_id = (text[i] - '0') + (yaobj->node_id * 10);
                    }
                    
                    free(text);
                }
            }
            else {
                yaobj->error = YAUID_ERROR_FILE_NODE_EXT;
                return yaobj;
            }
        }
        
        if(access( yaobj->c_lockfile, F_OK ) == -1)
        {
            if((yaobj->h_lockfile = fopen(yaobj->c_lockfile, "ab")) == 0)
            {
                yaobj->error = YAUID_ERROR_CREATE_KEY_FILE;
                return yaobj;
            }
            
            fclose(yaobj->h_lockfile);
        }
        
        if((yaobj->h_lockfile = fopen(yaobj->c_lockfile, "rb+")) == 0)
        {
            yaobj->error = YAUID_ERROR_OPEN_LOCK_FILE;
            return yaobj;
        }
        
        setbuf(yaobj->h_lockfile, NULL);
        
        yaobj->i_lockfile = fileno(yaobj->h_lockfile);
    }
    
    return yaobj;
}

void yauid_destroy(yauid* yaobj)
{
    if(yaobj == NULL)
        return;
    
    if(yaobj->h_lockfile)
        fclose(yaobj->h_lockfile);
    
    free(yaobj);
}

char * yauid_get_error_text_by_code(enum yauid_status error)
{
    if((YAUID_ERROR_TRY_COUNT_KEY - YAUID_OK) < error)
        return NULL;
    
    return error_text[error];
}

void yauid_set_node_id(yauid* yaobj, unsigned long node_id)
{
    yaobj->error = YAUID_OK;
    
    if(node_id < NUMBER_LIMIT_NODE)
    {
        yaobj->node_id = node_id;
        return;
    }
    
    yaobj->error = YAUID_ERROR_LONG_NODE_ID;
}

void yauid_set_sleep_usec(yauid* yaobj, useconds_t sleep_usec)
{
    yaobj->error = YAUID_OK;
    yaobj->sleep_usec = sleep_usec;
}

void yauid_set_try_count(yauid* yaobj, unsigned int try_count)
{
    yaobj->error = YAUID_OK;
    yaobj->try_count = try_count;
}

#endif


typedef yauid * Number__YAUID;

MODULE = Number::YAUID  PACKAGE = Number::YAUID

PROTOTYPES: DISABLE

Number::YAUID
init(perl_class, filepath_key, filepath_node_id)
	char *perl_class;
	SV *filepath_key;
	SV *filepath_node_id;
	
	CODE:
		if(SvOK(filepath_key) && SvOK(filepath_node_id))
		{
			RETVAL = yauid_init((char *)SvPV_nolen(filepath_key), (char *)SvPV_nolen(filepath_node_id));
		}
		else if(SvOK(filepath_key))
		{
			RETVAL = yauid_init((char *)SvPV_nolen(filepath_key), NULL);
		}
		else {
			RETVAL = NULL;
		}
		
	OUTPUT:
		RETVAL

SV*
get_key(obj)
	Number::YAUID obj;
	
	CODE:
		RETVAL = newSViv(yauid_get_key(obj));
		
	OUTPUT:
		RETVAL

SV*
get_key_once(obj)
	Number::YAUID obj;
	
	CODE:
		RETVAL = newSViv(yauid_get_key_once(obj));
		
	OUTPUT:
		RETVAL

void
DESTROY(obj)
	Number::YAUID obj;
	
	CODE:
		yauid_destroy(obj);

SV*
get_error_text_by_code(error_id)
	int error_id;
	
	CODE:
		char * text = yauid_get_error_text_by_code((enum yauid_status)(error_id));
		if(text)
		{
			RETVAL = newSVpv(text, 0);
		}
		else {
			RETVAL = &PL_sv_undef;
		}
		
	OUTPUT:
		RETVAL

SV*
get_error_code(obj)
	Number::YAUID obj;
	
	CODE:
		RETVAL = newSViv(obj->error);
	OUTPUT:
		RETVAL

SV*
set_node_id(obj, node_id)
	Number::YAUID obj;
	unsigned long node_id;
	
	CODE:
		yauid_set_node_id(obj, node_id);
		
		RETVAL = newSViv(YAUID_OK);
	OUTPUT:
		RETVAL

SV*
set_sleep_usec(obj, sleep_usec = 35000)
	Number::YAUID obj;
	size_t sleep_usec;
	
	CODE:
		yauid_set_sleep_usec(obj, (useconds_t)(sleep_usec));
		
		RETVAL = newSViv(YAUID_OK);
	OUTPUT:
		RETVAL

SV*
set_try_count(obj, try_count = 0)
	Number::YAUID obj;
	unsigned int try_count;
	
	CODE:
		yauid_set_try_count(obj, try_count);
		
		RETVAL = newSViv(YAUID_OK);
	OUTPUT:
		RETVAL

SV*
get_timestamp_by_key(obj, hkey)
	Number::YAUID obj;
	SV* hkey;
	
	CODE:
		RETVAL = newSViv( yauid_get_timestamp( (hkey_t)(SvIV(hkey)) ) );
	OUTPUT:
		RETVAL

SV*
get_node_id_by_key(obj, hkey = 0)
	Number::YAUID obj;
	SV* hkey;
	
	CODE:
		RETVAL = newSViv( yauid_get_node_id( (hkey_t)(SvIV(hkey)) ) );
	OUTPUT:
		RETVAL

SV*
get_inc_id_by_key(obj, hkey = 0)
	Number::YAUID obj;
	SV* hkey;
	
	CODE:
		RETVAL = newSViv( yauid_get_inc_id( (hkey_t)(SvIV(hkey)) ) );
	OUTPUT:
		RETVAL

SV*
get_max_inc()
	CODE:
		RETVAL = newSViv( yauid_get_max_inc() );
	OUTPUT:
		RETVAL

SV*
get_max_node_id()
	CODE:
		RETVAL = newSViv( yauid_get_max_node_id() );
	OUTPUT:
		RETVAL

SV*
get_max_timestamp()
	CODE:
		RETVAL = newSViv( yauid_get_max_timestamp() );
	OUTPUT:
		RETVAL
