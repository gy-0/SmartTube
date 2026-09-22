package com.liskovsoft.smartyoutubetv2.tv;

import android.app.Activity;
import android.os.Bundle;
import android.os.Handler;
import android.os.SystemClock;
import android.util.Log;
import android.view.TextureView;
import android.view.WindowManager;
import com.google.android.exoplayer2.ExoPlayerFactory;
import com.google.android.exoplayer2.ExoPlaybackException;
import com.google.android.exoplayer2.Player;
import com.google.android.exoplayer2.SimpleExoPlayer;
import com.google.android.exoplayer2.video.VideoListener;
import com.liskovsoft.smartyoutubetv2.common.exoplayer.ExoMediaSourceFactory;
import com.liskovsoft.smartyoutubetv2.common.prefs.PlayerTweaksData;
import java.util.Collections;

/** Debug-only, loopback-only playback fixture. Never launches or controls a physical device. */
public final class ParallelPlaybackTestActivity extends Activity {
    private static final String TAG = "ParallelPlaybackQA";
    private final Handler handler = new Handler();
    private SimpleExoPlayer player;
    private long started;
    private long firstFrameMs = -1;
    private int rebufferCount;
    private boolean played;
    private int connections;

    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        String url = getIntent().getStringExtra("url");
        // This test hook accepts only the emulator's host loopback alias or localhost.
        if (url == null || !(url.startsWith("http://10.0.2.2:") || url.startsWith("http://127.0.0.1:"))) {
            finish();
            return;
        }
        connections = getIntent().getIntExtra("connections", 4);
        PlayerTweaksData.instance(this).setParallelConnections(connections);
        getWindow().addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON);
        TextureView video = new TextureView(this);
        setContentView(video);
        player = ExoPlayerFactory.newSimpleInstance(this);
        player.setVolume(0);
        player.setVideoTextureView(video);
        started = SystemClock.elapsedRealtime();
        player.addVideoListener(new VideoListener() {
            @Override public void onRenderedFirstFrame() {
                if (firstFrameMs < 0) firstFrameMs = SystemClock.elapsedRealtime() - started;
                report("first-frame");
            }
        });
        player.addListener(new Player.EventListener() {
            @Override public void onPlayerStateChanged(boolean playWhenReady, int playbackState) {
                if (playbackState == Player.STATE_BUFFERING && played) rebufferCount++;
                if (playbackState == Player.STATE_READY) played = true;
                report(playbackState == Player.STATE_ENDED ? "ended" : "state");
            }
            @Override public void onPlayerError(ExoPlaybackException error) {
                Log.e(TAG, "Playback failed", error);
            }
        });
        player.prepare(new ExoMediaSourceFactory(this).fromUrlList(Collections.singletonList(url)));
        player.setPlayWhenReady(true);
        handler.post(tick);
    }

    private final Runnable tick = new Runnable() {
        @Override public void run() {
            if (player != null) {
                report("tick");
                handler.postDelayed(this, 1000);
            }
        }
    };

    private void report(String event) {
        Log.i(TAG, "event=" + event + " connections=" + connections
                + " elapsedMs=" + (SystemClock.elapsedRealtime() - started)
                + " firstFrameMs=" + firstFrameMs + " positionMs=" + player.getCurrentPosition()
                + " bufferedMs=" + player.getTotalBufferedDuration() + " state=" + player.getPlaybackState()
                + " rebuffers=" + rebufferCount);
    }

    @Override public void onDestroy() {
        handler.removeCallbacksAndMessages(null);
        if (player != null) {
            player.release();
            player = null;
        }
        super.onDestroy();
    }
}
