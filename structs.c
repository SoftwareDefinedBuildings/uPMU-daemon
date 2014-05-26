typedef struct {
    float angle;
    float mag;
} sync_point;
 
typedef struct sync_output_msgq {
    float sampleRate;
    int times[6];
    int lockstate[120];
    sync_point L1MagAng[120];
    sync_point L2MagAng[120];
    sync_point L3MagAng[120];
    sync_point C1MagAng[120];
    sync_point C2MagAng[120];
    sync_point C3MagAng[120];
} sync_output_msgq;
 
typedef struct sync_pll_stats_msgq
{
    unsigned int ppl_state;
    unsigned int pps_prd;
    int curr_err;
    int center_frq_offset;
} sync_pll_stats_msgq;
 
typedef struct  {
    float alt;
    float lat;
    float hdop;
    float lon;
    float satellites;
    float state;
    float hasFix;
} sync_gps_stats;
 
 
typedef struct sync_output {
    sync_output_msgq sync_data;
    sync_pll_stats_msgq pll_stats;
    sync_gps_stats gps_stats;
} sync_output;
